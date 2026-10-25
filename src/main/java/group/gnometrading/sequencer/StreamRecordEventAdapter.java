package group.gnometrading.sequencer;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Adapts an {@link EventHandler}{@code <StreamRecord>} for use as a {@link SequencedEventHandler}.
 *
 * <p>On each event, looks up the pre-allocated record of the event's template, wraps it around the SequencedEvent's
 * buffer memory (zero-copy), and delegates to the underlying handler. The record is safe to use only within the
 * handler callback.
 *
 * <p>Handles every stream record type, of every family; any other template (a core message) throws.
 */
public final class StreamRecordEventAdapter implements SequencedEventHandler {

    private final StreamRecord[] recordsByTemplateId;
    private final EventHandler<StreamRecord> delegate;

    /**
     * Constructs a new adapter wrapping the given delegate handler.
     *
     * @param delegate the handler to receive unwrapped stream records
     */
    public StreamRecordEventAdapter(EventHandler<StreamRecord> delegate) {
        this.delegate = delegate;
        this.recordsByTemplateId = RecordType.newRecordsByTemplateId();
    }

    @Override
    public void onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length)
            throws Exception {
        StreamRecord record =
                templateId >= 0 && templateId < recordsByTemplateId.length ? recordsByTemplateId[templateId] : null;
        if (record == null) {
            throw new IllegalArgumentException("No stream record for templateId: " + templateId);
        }
        record.wrap(buffer);
        delegate.onEvent(record, globalSequence, false);
    }
}
