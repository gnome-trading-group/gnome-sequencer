package group.gnometrading.sequencer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Persists sequenced events from one or more {@link SequencedRingBuffer} instances to a
 * journal: a directory of memory-mapped segment files, {@code segment-00000.bin} onwards.
 *
 * <p>Since producers independently grab global sequence numbers before publishing to their
 * respective ring buffers, events may arrive at this writer out of global sequence order.
 * A pre-allocated fixed-size reorder buffer absorbs this skew and flushes contiguous runs
 * in global sequence order to disk.
 *
 * <p>Journal record format (little-endian):
 * <pre>
 * [8 bytes] globalSequence (uint64)
 * [2 bytes] sourceId      (uint16)
 * [2 bytes] templateId    (uint16)
 * [2 bytes] payloadLength (uint16)
 * [N bytes] SBE payload
 * </pre>
 *
 * <p>{@code sourceId} names the buffer an event was read from, chosen by whoever wires the
 * journal. Several buffers carry copies of the same message (an order and its routed copy),
 * and replay needs to tell them apart.
 *
 * <p>A journal may be opened to {@linkplain #JournalWriter(Path, long, Consumer, boolean) skip lost
 * events}, for buffers read by {@link LappingPoller}s, which drop what they are lapped past. Global
 * sequences that never arrive are then written as one gap record per missing run instead of stalling
 * or failing the journal: source {@link #GAP_SOURCE_ID}, template {@link #GAP_TEMPLATE_ID}, the
 * header's sequence the first one lost and an 8-byte payload the last one lost. A lost event that
 * arrives after its gap was written is dropped and counted. Without that option every global
 * sequence must arrive, and a missing one fails the journal.
 *
 * <p>A segment ends when the next record doesn't fit or when {@link #roll()} is called, and is handed to the
 * segment listener to {@link JournalSegment#seal seal}, so another process can ship the journal while the session
 * runs. A record never spans two segments, and a segment with no records is never handed on.
 *
 * <p>All fields are pre-allocated at construction time. {@link #append} is zero-allocation except when it starts
 * a new segment. Nothing is forced to disk: the mapped pages outlive the process in the page cache, so a crash loses
 * no records, and the open segment can be shipped as it is.
 *
 * <p>Not thread-safe: one agent polls every journaled buffer and calls {@link #append}, {@link #roll} and
 * {@link #close}.
 */
public final class JournalWriter implements Closeable {

    static final int HEADER_SIZE = Long.BYTES + Short.BYTES + Short.BYTES + Short.BYTES;
    /** The source id of a gap record; no journaled buffer may use it. */
    public static final int GAP_SOURCE_ID = 0xFFFF;
    /** The template id of a gap record. */
    public static final int GAP_TEMPLATE_ID = 0;

    static final int GAP_PAYLOAD_SIZE = Long.BYTES;

    // Reorder buffer sized as a power-of-2 to allow bitwise-mask indexing.
    private static final int REORDER_BUFFER_CAPACITY = 1 << 8;
    private static final int REORDER_BUFFER_MASK = REORDER_BUFFER_CAPACITY - 1;

    private final long[] reorderSequences;
    private final int[] reorderSourceIds;
    private final int[] reorderTemplateIds;
    private final int[] reorderLengths;
    private final UnsafeBuffer[] reorderPayloads;

    private final Path directory;
    private final long segmentSizeBytes;
    private final Consumer<JournalSegment> segmentListener;
    private final boolean skipsLost;

    private MappedByteBuffer mappedBuffer;
    private Path segmentPath;
    private int segmentIndex;
    private long segmentFirstSequence;
    private long lastFlushedSequence;
    private long highestReceived;
    private long lostSequences;
    private long lateEvents;
    private boolean closed;

    /**
     * Opens a journal in {@code directory}, starting with its first segment.
     *
     * @param directory where the segment files go; created if missing
     * @param segmentSizeBytes the size of each memory-mapped segment in bytes
     * @param segmentListener called on the writing thread with each finished segment; must hand it off quickly
     * @throws IOException if the directory or first segment cannot be created
     */
    public JournalWriter(Path directory, long segmentSizeBytes, Consumer<JournalSegment> segmentListener)
            throws IOException {
        this(directory, segmentSizeBytes, segmentListener, false);
    }

    /**
     * Opens a journal in {@code directory}, starting with its first segment.
     *
     * @param directory where the segment files go; created if missing
     * @param segmentSizeBytes the size of each memory-mapped segment in bytes
     * @param segmentListener called on the writing thread with each finished segment; must hand it off quickly
     * @param skipsLost whether global sequences that never arrive are written as gaps rather than failing the journal
     * @throws IOException if the directory or first segment cannot be created
     */
    public JournalWriter(
            Path directory, long segmentSizeBytes, Consumer<JournalSegment> segmentListener, boolean skipsLost)
            throws IOException {
        if (segmentSizeBytes < HEADER_SIZE + SequencedEvent.MAX_MESSAGE_SIZE || segmentSizeBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "A segment must hold the largest record and fit one mapping: " + segmentSizeBytes + " bytes");
        }
        this.reorderSequences = new long[REORDER_BUFFER_CAPACITY];
        this.reorderSourceIds = new int[REORDER_BUFFER_CAPACITY];
        this.reorderTemplateIds = new int[REORDER_BUFFER_CAPACITY];
        this.reorderLengths = new int[REORDER_BUFFER_CAPACITY];
        this.reorderPayloads = new UnsafeBuffer[REORDER_BUFFER_CAPACITY];
        for (int i = 0; i < REORDER_BUFFER_CAPACITY; i++) {
            this.reorderPayloads[i] = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        }
        this.directory = directory;
        this.segmentSizeBytes = segmentSizeBytes;
        this.segmentListener = segmentListener;
        this.skipsLost = skipsLost;
        this.lastFlushedSequence = 0;
        this.closed = false;
        Files.createDirectories(directory);
        openSegment(0);
    }

    /** The file name of segment {@code index} while it is written, so readers can find segments in order. */
    public static String segmentFileName(int index) {
        return String.format("segment-%05d.bin", index);
    }

    /** The file name of segment {@code index} once {@link JournalSegment#seal sealed}. */
    public static String sealedFileName(int index) {
        return String.format("segment-%05d.done", index);
    }

    /**
     * Returns a handler that appends every event it receives under {@code sourceId}, for passing
     * to {@link SequencedRingBuffer#createPoller}. Allocates; call during wiring only.
     *
     * @param sourceId the id recorded with each event from this source
     * @return a handler bound to this writer and source
     */
    public SequencedEventHandler source(int sourceId) {
        return (globalSequence, templateId, buffer, length) ->
                append(sourceId, globalSequence, templateId, buffer, length);
    }

    /**
     * Buffers one event and writes every record that is now contiguous in global sequence order. When skipping lost
     * events, one too far ahead of the last written record to fit in the reorder buffer first writes the oldest
     * missing sequences as lost, and one at or before the last written record is dropped.
     *
     * @throws IllegalStateException unless skipping lost events: if the event was already journaled, or if it is too
     *     far ahead of the last written record to fit in the reorder buffer — which means some earlier global
     *     sequence number was taken by a buffer that is not journaled
     */
    public void append(int sourceId, long globalSequence, int templateId, UnsafeBuffer buffer, int length) {
        if (closed) {
            return;
        }
        if (globalSequence <= lastFlushedSequence) {
            if (skipsLost) {
                lateEvents++;
                return;
            }
            throw new IllegalStateException("Journal already wrote global sequence " + globalSequence);
        }
        if (globalSequence > lastFlushedSequence + REORDER_BUFFER_CAPACITY) {
            if (!skipsLost) {
                throw new IllegalStateException("Journal gap: still waiting for global sequence "
                        + (lastFlushedSequence + 1) + " but received " + globalSequence
                        + "; a buffer on this sequence is not journaled");
            }
            skipLostBelow(globalSequence - REORDER_BUFFER_CAPACITY + 1);
        }
        int slot = (int) (globalSequence & REORDER_BUFFER_MASK);
        reorderSequences[slot] = globalSequence;
        reorderSourceIds[slot] = sourceId;
        reorderTemplateIds[slot] = templateId;
        reorderLengths[slot] = length;
        reorderPayloads[slot].putBytes(0, buffer, 0, length);
        highestReceived = Math.max(highestReceived, globalSequence);
        flushContiguous();
    }

    /**
     * Writes every global sequence still missing below the highest one received as lost, and with it every record
     * that was waiting behind them. For when a reader was lapped: what it skipped will never arrive. Poll every
     * journaled buffer first, so events merely not read yet aren't taken for lost.
     *
     * @return how many global sequences were written as lost
     * @throws IllegalStateException if the journal wasn't opened to skip lost events
     */
    public long skipMissing() {
        if (!skipsLost) {
            throw new IllegalStateException("This journal requires every global sequence; it can't skip any");
        }
        if (closed) {
            return 0;
        }
        final long before = lostSequences;
        skipLostBelow(highestReceived + 1);
        return lostSequences - before;
    }

    private void skipLostBelow(long below) {
        while (lastFlushedSequence + 1 < below) {
            final long first = lastFlushedSequence + 1;
            if (!isBuffered(first)) {
                // Nothing past the reorder buffer's reach can be buffered yet, so a run reaching it runs to the end.
                final long scanEnd = Math.min(below - 1, lastFlushedSequence + REORDER_BUFFER_CAPACITY);
                long last = first;
                while (last < scanEnd && !isBuffered(last + 1)) {
                    last++;
                }
                writeGap(first, last == scanEnd ? below - 1 : last);
            }
            flushContiguous();
        }
    }

    private boolean isBuffered(long globalSequence) {
        return reorderSequences[(int) (globalSequence & REORDER_BUFFER_MASK)] == globalSequence;
    }

    private void writeGap(long first, long last) {
        MappedByteBuffer mapped = mappedBuffer;
        if (mapped.remaining() < HEADER_SIZE + GAP_PAYLOAD_SIZE) {
            roll();
            mapped = mappedBuffer;
        }
        if (mapped.position() == 0) {
            segmentFirstSequence = first;
        }
        mapped.putLong(first);
        mapped.putShort((short) GAP_SOURCE_ID);
        mapped.putShort((short) GAP_TEMPLATE_ID);
        mapped.putShort((short) GAP_PAYLOAD_SIZE);
        mapped.putLong(last);
        lastFlushedSequence = last;
        lostSequences += last - first + 1;
    }

    private void flushContiguous() {
        while (true) {
            long nextSeq = lastFlushedSequence + 1;
            int slot = (int) (nextSeq & REORDER_BUFFER_MASK);
            if (reorderSequences[slot] != nextSeq) {
                break;
            }
            int length = reorderLengths[slot];
            MappedByteBuffer mapped = mappedBuffer;
            if (mapped.remaining() < HEADER_SIZE + length) {
                roll();
                mapped = mappedBuffer;
            }
            if (mapped.position() == 0) {
                segmentFirstSequence = nextSeq;
            }
            mapped.putLong(nextSeq);
            mapped.putShort((short) reorderSourceIds[slot]);
            mapped.putShort((short) reorderTemplateIds[slot]);
            mapped.putShort((short) length);
            for (int i = 0; i < length; i++) {
                mapped.put(reorderPayloads[slot].getByte(i));
            }
            lastFlushedSequence = nextSeq;
        }
    }

    /**
     * Ends the open segment and starts the next one, handing the finished one to the segment listener. Does nothing
     * while the open segment has no records.
     *
     * @throws UncheckedIOException if the next segment cannot be created
     */
    public void roll() {
        if (closed || mappedBuffer.position() == 0) {
            return;
        }
        JournalSegment finished = finishSegment();
        try {
            openSegment(segmentIndex + 1);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start journal segment " + (segmentIndex + 1), e);
        }
        segmentListener.accept(finished);
    }

    private JournalSegment finishSegment() {
        return new JournalSegment(
                segmentIndex,
                segmentPath,
                mappedBuffer.position(),
                segmentFirstSequence,
                lastFlushedSequence,
                mappedBuffer);
    }

    private void openSegment(int index) throws IOException {
        Path path = directory.resolve(segmentFileName(index));
        try (FileChannel channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.truncate(0);
            channel.truncate(segmentSizeBytes);
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSizeBytes);
            mapped.order(ByteOrder.LITTLE_ENDIAN);
            this.mappedBuffer = mapped;
        }
        this.segmentPath = path;
        this.segmentIndex = index;
    }

    /**
     * Returns the last global sequence number flushed to the journal.
     *
     * @return the last flushed sequence
     */
    public long lastFlushedSequence() {
        return lastFlushedSequence;
    }

    /** Global sequences written as lost. */
    public long lostSequences() {
        return lostSequences;
    }

    /** Events dropped because they arrived after their sequence was written as lost. */
    public long lateEvents() {
        return lateEvents;
    }

    /** Returns the number of bytes written to the open segment so far. */
    public int segmentWrittenBytes() {
        return mappedBuffer.position();
    }

    /**
     * Hands the open segment to the segment listener if it holds any records; one without records is deleted.
     * Nothing is written after. When skipping lost events, whatever is still missing is written as lost first, so
     * no record waiting behind it is left out.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (skipsLost) {
            skipMissing();
        }
        closed = true;
        if (mappedBuffer.position() == 0) {
            IoUtil.unmap(mappedBuffer);
            try {
                Files.deleteIfExists(segmentPath);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to delete empty journal segment " + segmentPath, e);
            }
            return;
        }
        segmentListener.accept(finishSegment());
    }
}
