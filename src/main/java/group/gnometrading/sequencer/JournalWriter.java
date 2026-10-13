package group.gnometrading.sequencer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Persists sequenced events from one or more {@link SequencedRingBuffer} instances to a
 * memory-mapped journal file.
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
 * <p>All fields are pre-allocated at construction time. {@link #append} is zero-allocation.
 * File is flushed on close; no fsync per event.
 *
 * <p>Not thread-safe: one agent polls every journaled buffer and calls {@link #append},
 * {@link #flush} and {@link #close}.
 */
public final class JournalWriter implements Closeable {

    static final int HEADER_SIZE = Long.BYTES + Short.BYTES + Short.BYTES + Short.BYTES;

    // Reorder buffer sized as a power-of-2 to allow bitwise-mask indexing.
    private static final int REORDER_BUFFER_CAPACITY = 1 << 8;
    private static final int REORDER_BUFFER_MASK = REORDER_BUFFER_CAPACITY - 1;

    private final long[] reorderSequences;
    private final int[] reorderSourceIds;
    private final int[] reorderTemplateIds;
    private final int[] reorderLengths;
    private final UnsafeBuffer[] reorderPayloads;

    private final MappedByteBuffer mappedBuffer;
    private long lastFlushedSequence;
    private boolean closed;

    /**
     * Opens a new journal file at the given path.
     *
     * @param path the journal file path to create or overwrite
     * @param fileSizeBytes the size of the memory-mapped region in bytes
     * @throws IOException if the file cannot be opened or mapped
     */
    public JournalWriter(Path path, long fileSizeBytes) throws IOException {
        this.reorderSequences = new long[REORDER_BUFFER_CAPACITY];
        this.reorderSourceIds = new int[REORDER_BUFFER_CAPACITY];
        this.reorderTemplateIds = new int[REORDER_BUFFER_CAPACITY];
        this.reorderLengths = new int[REORDER_BUFFER_CAPACITY];
        this.reorderPayloads = new UnsafeBuffer[REORDER_BUFFER_CAPACITY];
        for (int i = 0; i < REORDER_BUFFER_CAPACITY; i++) {
            this.reorderPayloads[i] = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        }
        this.lastFlushedSequence = 0;
        this.closed = false;

        try (FileChannel channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.truncate(fileSizeBytes);
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSizeBytes);
            this.mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
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
     * Buffers one event and writes every record that is now contiguous in global sequence order.
     *
     * @throws IllegalStateException if the event was already journaled, or if it is too far ahead
     *     of the last written record to fit in the reorder buffer — which means some earlier global
     *     sequence number was taken by a buffer that is not journaled
     */
    public void append(int sourceId, long globalSequence, int templateId, UnsafeBuffer buffer, int length) {
        if (closed) {
            return;
        }
        if (globalSequence <= lastFlushedSequence) {
            throw new IllegalStateException("Journal already wrote global sequence " + globalSequence);
        }
        if (globalSequence > lastFlushedSequence + REORDER_BUFFER_CAPACITY) {
            throw new IllegalStateException("Journal gap: still waiting for global sequence "
                    + (lastFlushedSequence + 1) + " but received " + globalSequence
                    + "; a buffer on this sequence is not journaled");
        }
        int slot = (int) (globalSequence & REORDER_BUFFER_MASK);
        reorderSequences[slot] = globalSequence;
        reorderSourceIds[slot] = sourceId;
        reorderTemplateIds[slot] = templateId;
        reorderLengths[slot] = length;
        reorderPayloads[slot].putBytes(0, buffer, 0, length);
        flushContiguous();
    }

    private void flushContiguous() {
        while (true) {
            long nextSeq = lastFlushedSequence + 1;
            int slot = (int) (nextSeq & REORDER_BUFFER_MASK);
            if (reorderSequences[slot] != nextSeq) {
                break;
            }
            int length = reorderLengths[slot];
            mappedBuffer.putLong(nextSeq);
            mappedBuffer.putShort((short) reorderSourceIds[slot]);
            mappedBuffer.putShort((short) reorderTemplateIds[slot]);
            mappedBuffer.putShort((short) length);
            for (int i = 0; i < length; i++) {
                mappedBuffer.put(reorderPayloads[slot].getByte(i));
            }
            lastFlushedSequence = nextSeq;
        }
    }

    /**
     * Returns the last global sequence number flushed to the journal.
     *
     * @return the last flushed sequence
     */
    public long lastFlushedSequence() {
        return lastFlushedSequence;
    }

    /**
     * Returns the number of bytes written to the journal file so far.
     * Safe to call after {@link #close()} to determine how many bytes to compress and upload.
     */
    public int writtenBytes() {
        return mappedBuffer.position();
    }

    /**
     * Forces any dirty pages to disk. Called periodically to protect against data loss on hard
     * crash.
     */
    public void flush() {
        mappedBuffer.force();
    }

    @Override
    public void close() {
        closed = true;
        mappedBuffer.force();
    }
}
