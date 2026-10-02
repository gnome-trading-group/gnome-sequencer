package group.gnometrading.sequencer;

import group.gnometrading.schemas.MessageHeaderDecoder;
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
 * Reads a journal file written by {@link JournalWriter} and replays records
 * in global sequence order via a callback.
 *
 * <p>Reading is sequential and zero-allocation on the hot path. The payload buffer
 * passed to the callback is a pre-allocated flyweight that is reused across calls;
 * callers must not retain a reference beyond the callback.
 *
 * <p>Records written by an older schema version are migrated to the current one before the callback
 * sees them, so a journal outlives schema changes. Only those records are copied.
 */
public final class JournalReader implements Closeable {

    private static final int UINT16_MASK = 0xFFFF;

    private final MappedByteBuffer mappedBuffer;
    private final UnsafeBuffer payloadFlyweight;
    // A migrated message has the current layout, so it fits the largest current message.
    private final UnsafeBuffer migratedPayload = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
    private long lastReadSequence;

    /**
     * Opens the journal file at the given path for reading.
     *
     * @param path the journal file path to read
     * @throws IOException if the file cannot be opened or mapped
     */
    public JournalReader(Path path) throws IOException {
        this.payloadFlyweight = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        this.lastReadSequence = 0;

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            this.mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    /**
     * Reads all remaining records from the journal and dispatches each to the handler.
     *
     * <p>Records are validated to be monotonically increasing. The {@code buffer} parameter
     * passed to the handler is a reused flyweight — do not retain it across calls.
     *
     * @param handler the callback to invoke for each record
     * @throws IllegalStateException if a sequence gap or regression is detected
     */
    public void readAll(JournalRecordHandler handler) {
        while (mappedBuffer.remaining() >= JournalWriter.HEADER_SIZE) {
            long globalSequence = mappedBuffer.getLong();
            if (globalSequence == 0) {
                break;
            }

            int templateId = Short.toUnsignedInt(mappedBuffer.getShort());
            int length = Short.toUnsignedInt(mappedBuffer.getShort());

            if (globalSequence != lastReadSequence + 1) {
                throw new IllegalStateException(
                        "Journal sequence gap: expected " + (lastReadSequence + 1) + " but got " + globalSequence);
            }

            if (mappedBuffer.remaining() < length) {
                break;
            }

            mappedBuffer.get(payloadFlyweight.byteArray(), 0, length);
            payloadFlyweight.wrap(payloadFlyweight.byteArray(), 0, length);

            lastReadSequence = globalSequence;
            if (isOlderGnomeMessage(length)) {
                final int migratedLength = SbeMigrator.migrateMessage(payloadFlyweight, 0, migratedPayload, 0);
                handler.onRecord(globalSequence, templateId, migratedPayload, migratedLength);
            } else {
                handler.onRecord(globalSequence, templateId, payloadFlyweight, length);
            }
        }
    }

    private boolean isOlderGnomeMessage(final int length) {
        return length >= MessageHeaderDecoder.ENCODED_LENGTH
                && (payloadFlyweight.getShort(MessageHeaderDecoder.schemaIdEncodingOffset(), ByteOrder.LITTLE_ENDIAN)
                                & UINT16_MASK)
                        == MessageHeaderDecoder.SCHEMA_ID
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
