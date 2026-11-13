package group.gnometrading.sequencer;

import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.schemas.migration.SbeMigrator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Reads one journal segment written by {@link JournalWriter} and replays its records
 * in global sequence order via a callback.
 *
 * <p>Reading is sequential and zero-allocation on the hot path. The payload buffer
 * passed to the callback is a pre-allocated flyweight that is reused across calls;
 * callers must not retain a reference beyond the callback.
 *
 * <p>Reads both record formats: a segment starting with {@link JournalWriter}'s header holds records with their publish
 * time; one from before the header holds none, and its records read with a time of {@link #NO_TIMESTAMP}.
 *
 * <p>Records written by an older version of their schema are migrated to its current one before the callback
 * sees them, so a journal outlives schema changes. Only those records are copied.
 */
public final class JournalReader implements Closeable {

    /** The time of a record from a segment written before records carried one. */
    public static final long NO_TIMESTAMP = 0;

    private static final int LEGACY_HEADER_SIZE = Long.BYTES + Short.BYTES + Short.BYTES + Short.BYTES;

    private final MappedByteBuffer mappedBuffer;
    private final UnsafeBuffer payloadFlyweight;
    private final boolean timed;
    // A migrated message has the current layout, so it fits the largest current message.
    private final UnsafeBuffer migratedPayload = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
    private long lastReadSequence;

    /**
     * Opens the first segment of a journal for reading.
     *
     * @param path the segment file to read
     * @throws IOException if the file cannot be opened or mapped
     */
    public JournalReader(Path path) throws IOException {
        this(path, 0);
    }

    /**
     * Opens a later segment of a journal for reading, so its first record must follow the previous segment's last.
     *
     * @param path the segment file to read
     * @param previousSequence the last global sequence of the segment before it
     * @throws IOException if the file cannot be opened or mapped
     */
    public JournalReader(Path path, long previousSequence) throws IOException {
        this.payloadFlyweight = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        this.lastReadSequence = previousSequence;

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            this.mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.timed = mappedBuffer.remaining() >= JournalWriter.SEGMENT_HEADER_SIZE
                && mappedBuffer.getInt(0) == JournalWriter.SEGMENT_MAGIC;
        if (timed) {
            final int format = Short.toUnsignedInt(mappedBuffer.getShort(Integer.BYTES));
            if (format != JournalWriter.FORMAT) {
                throw new IllegalStateException("Journal segment " + path + " has format " + format
                        + "; this reader reads up to " + JournalWriter.FORMAT);
            }
            mappedBuffer.position(JournalWriter.SEGMENT_HEADER_SIZE);
        }
    }

    /**
     * Reads all remaining records from the journal and dispatches each to the handler.
     *
     * <p>Records are validated to be monotonically increasing; a gap record goes to
     * {@link JournalRecordHandler#onGap}. The {@code buffer} parameter passed to the handler is a
     * reused flyweight — do not retain it across calls.
     *
     * @param handler the callback to invoke for each record
     * @throws IllegalStateException if a sequence gap or regression is detected
     */
    public void readAll(JournalRecordHandler handler) {
        final int headerSize = timed ? JournalWriter.HEADER_SIZE : LEGACY_HEADER_SIZE;
        while (mappedBuffer.remaining() >= headerSize) {
            long globalSequence = mappedBuffer.getLong();
            if (globalSequence == 0) {
                break;
            }
            long timestampNanos = timed ? mappedBuffer.getLong() : NO_TIMESTAMP;

            int sourceId = Short.toUnsignedInt(mappedBuffer.getShort());
            int templateId = Short.toUnsignedInt(mappedBuffer.getShort());
            int length = Short.toUnsignedInt(mappedBuffer.getShort());

            if (globalSequence != lastReadSequence + 1) {
                throw new IllegalStateException(
                        "Journal sequence gap: expected " + (lastReadSequence + 1) + " but got " + globalSequence);
            }

            if (mappedBuffer.remaining() < length) {
                break;
            }

            if (sourceId == JournalWriter.GAP_SOURCE_ID && templateId == JournalWriter.GAP_TEMPLATE_ID) {
                readGap(globalSequence, length, handler);
                continue;
            }

            mappedBuffer.get(payloadFlyweight.byteArray(), 0, length);
            payloadFlyweight.wrap(payloadFlyweight.byteArray(), 0, length);

            lastReadSequence = globalSequence;
            if (isOlderGnomeMessage(length)) {
                final int migratedLength = SbeMigrator.migrateMessage(payloadFlyweight, 0, migratedPayload, 0);
                handler.onRecord(globalSequence, timestampNanos, sourceId, templateId, migratedPayload, migratedLength);
            } else {
                handler.onRecord(globalSequence, timestampNanos, sourceId, templateId, payloadFlyweight, length);
            }
        }
    }

    private void readGap(long firstLost, int length, JournalRecordHandler handler) {
        if (length != JournalWriter.GAP_PAYLOAD_SIZE) {
            throw new IllegalStateException("Journal gap record at " + firstLost + " has " + length + " bytes");
        }
        long lastLost = mappedBuffer.getLong();
        if (lastLost < firstLost) {
            throw new IllegalStateException("Journal gap record ends at " + lastLost + " before " + firstLost);
        }
        lastReadSequence = lastLost;
        handler.onGap(firstLost, lastLost);
    }

    private boolean isOlderGnomeMessage(final int length) {
        return length >= SbeMessage.HEADER_LENGTH
                && SbeMigrator.isGnomeSchema(SbeMessage.schemaId(payloadFlyweight, 0))
                && SbeMigrator.needsMigration(payloadFlyweight, 0);
    }

    /**
     * Returns the last global sequence number successfully read.
     *
     * @return the last read sequence
     */
    public long lastReadSequence() {
        return lastReadSequence;
    }

    @Override
    public void close() {}
}
