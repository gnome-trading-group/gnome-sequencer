package group.gnometrading.sequencer;

import com.lmax.disruptor.EventHandler;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
 *
 * <p>Thread-safe: {@link #onEvent}, {@link #close}, and {@link #lastFlushedSequence} are
 * guarded by a {@link ReentrantLock} to allow a single instance to be attached to multiple ring
 * buffers whose Disruptor consumer threads call {@code onEvent} concurrently. {@link #flush} is
 * intentionally lock-free — {@link MappedByteBuffer#force} is safe to invoke concurrently with
 * writes.
 */
public final class JournalWriter implements EventHandler<SequencedEvent>, Closeable {

    static final int HEADER_SIZE = Long.BYTES + Short.BYTES + Short.BYTES;

    // Reorder buffer sized as a power-of-2 to allow bitwise-mask indexing.
    private static final int REORDER_BUFFER_CAPACITY = 1 << 8;
    private static final int REORDER_BUFFER_MASK = REORDER_BUFFER_CAPACITY - 1;

    private final long[] reorderSequences;
    private final int[] reorderTemplateIds;
    private final int[] reorderLengths;
    private final UnsafeBuffer[] reorderPayloads;

    private final MappedByteBuffer mappedBuffer;
    private final Lock lock = new ReentrantLock();
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

    @Override
    public void onEvent(SequencedEvent event, long sequence, boolean endOfBatch) {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            int slot = (int) (event.globalSequence & REORDER_BUFFER_MASK);
            reorderSequences[slot] = event.globalSequence;
            reorderTemplateIds[slot] = event.templateId;
            reorderLengths[slot] = event.bufferLength;
            reorderPayloads[slot].putBytes(0, event.buffer, 0, event.bufferLength);
            flushContiguous();
        } finally {
            lock.unlock();
        }
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
        lock.lock();
        try {
            return lastFlushedSequence;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of bytes written to the journal file so far.
     * Safe to call after {@link #close()} to determine how many bytes to compress and upload.
     */
    public int writtenBytes() {
        lock.lock();
        try {
            return mappedBuffer.position();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Forces any dirty pages to disk without stopping event processing.
     * Called periodically by an off-hot-path timer to protect against data loss on hard crash.
     * Not synchronized — {@link MappedByteBuffer#force} is safe to call concurrently with writes.
     */
    public void flush() {
        mappedBuffer.force();
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
        } finally {
            lock.unlock();
        }
        mappedBuffer.force();
    }
}
