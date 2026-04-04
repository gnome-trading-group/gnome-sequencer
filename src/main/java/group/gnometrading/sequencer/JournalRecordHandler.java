package group.gnometrading.sequencer;

import org.agrona.concurrent.UnsafeBuffer;

/**
 * Callback invoked by {@link JournalReader} for each record in the journal.
 *
 * <p>The {@code buffer} is a reused flyweight. Do not retain a reference to it beyond
 * the scope of this callback.
 */
@FunctionalInterface
public interface JournalRecordHandler {

    /**
     * Called once per journal record in global sequence order.
     *
     * @param globalSequence the monotonically increasing global sequence number
     * @param templateId the SBE template ID identifying the message type
     * @param buffer the SBE payload bytes (flyweight, do not retain)
     * @param length the number of valid bytes in {@code buffer}
     */
    void onRecord(long globalSequence, int templateId, UnsafeBuffer buffer, int length);
}
