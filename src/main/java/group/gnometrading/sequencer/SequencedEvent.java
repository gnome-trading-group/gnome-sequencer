package group.gnometrading.sequencer;

import com.lmax.disruptor.EventFactory;
import group.gnometrading.schemas.Mbp10Encoder;
import group.gnometrading.schemas.MessageHeaderEncoder;
import group.gnometrading.utils.ByteBufferUtils;
import org.agrona.concurrent.UnsafeBuffer;

public final class SequencedEvent {

    // Sized to fit the largest SBE message (Mbp10: 374 block + 8 header).
    static final int MAX_MESSAGE_SIZE = MessageHeaderEncoder.ENCODED_LENGTH + Mbp10Encoder.BLOCK_LENGTH;

    public static final EventFactory<SequencedEvent> FACTORY = SequencedEvent::new;

    public long globalSequence;
    public int templateId;
    public int bufferLength;
    public final UnsafeBuffer buffer;

    public SequencedEvent() {
        this.buffer = ByteBufferUtils.createAlignedUnsafeBuffer(MAX_MESSAGE_SIZE);
    }
}
