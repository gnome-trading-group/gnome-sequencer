package group.gnometrading.sequencer;

import com.lmax.disruptor.EventFactory;
import group.gnometrading.utils.ByteBufferUtils;
import org.agrona.concurrent.UnsafeBuffer;

public final class SequencedEvent {

    // A fixed size rather than the largest message's, so a family can add a record without resizing every ring and
    // journal; SchemaSlotSizeTest holds every message of every schema to it. Metric rows are capped to fit it.
    public static final int MAX_MESSAGE_SIZE = 1024;

    public static final EventFactory<SequencedEvent> FACTORY = SequencedEvent::new;

    public long globalSequence;
    public int templateId;
    public int bufferLength;
    public final UnsafeBuffer buffer;

    public SequencedEvent() {
        this.buffer = ByteBufferUtils.createAlignedUnsafeBuffer(MAX_MESSAGE_SIZE);
    }
}
