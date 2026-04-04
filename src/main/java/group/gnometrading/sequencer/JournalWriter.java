package group.gnometrading.sequencer;

import com.lmax.disruptor.EventHandler;
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
 * [2 bytes] templateId    (uint16)
 * [2 bytes] payloadLength (uint16)
 * [N bytes] SBE payload
 * </pre>
 *
 * <p>All fields are pre-allocated at construction time. {@link #onEvent} is zero-allocation.
 * File is flushed on close; no fsync per event.
 */
public final class JournalWriter implements EventHandler<SequencedEvent>, Closeable {

    static final int HEADER_SIZE = Long.BYTES + Short.BYTES + Short.BYTES;

    // Reorder buffer sized as a power-of-2 to allow bitwise-mask indexing.
    private static final int REORDER_BUFFER_CAPACITY = 1 << 6;
    private static final int REORDER_BUFFER_MASK = REORDER_BUFFER_CAPACITY - 1;

    private final long[] reorderSequences;
    private final int[] reorderTemplateIds;
    private final int[] reorderLengths;
    private final UnsafeBuffer[] reorderPayloads;

    private final MappedByteBuffer mappedBuffer;
    private long lastFlushedSequence;

    /**
     * Opens a new journal file at the given path.
     *
     * @param path the journal file path to create or overwrite
     * @param fileSizeBytes the size of the memory-mapped region in bytes
     * @throws IOException if the file cannot be opened or mapped
     */
    public JournalWriter(Path path, long fileSizeBytes) throws IOException {
        this.reorderSequences = new long[REORDER_BUFFER_CAPACITY];
        this.reorderTemplateIds = new int[REORDER_BUFFER_CAPACITY];
        this.reorderLengths = new int[REORDER_BUFFER_CAPACITY];
        this.reorderPayloads = new UnsafeBuffer[REORDER_BUFFER_CAPACITY];
        for (int i = 0; i < REORDER_BUFFER_CAPACITY; i++) {
            this.reorderPayloads[i] = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        }
        this.lastFlushedSequence = 0;

        try (FileChannel channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.truncate(fileSizeBytes);
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSizeBytes);
            this.mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    @Override
    public void onEvent(SequencedEvent event, long sequence, boolean endOfBatch) {
        int slot = (int) (event.globalSequence & REORDER_BUFFER_MASK);
        reorderSequences[slot] = event.globalSequence;
        reorderTemplateIds[slot] = event.templateId;
        reorderLengths[slot] = event.bufferLength;
        reorderPayloads[slot].putBytes(0, event.buffer, 0, event.bufferLength);
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

    @Override
    public void close() {
        mappedBuffer.force();
    }
}
