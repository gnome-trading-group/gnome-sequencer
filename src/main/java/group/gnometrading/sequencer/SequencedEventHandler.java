package group.gnometrading.sequencer;

import com.lmax.disruptor.EventHandler;
import org.agrona.concurrent.UnsafeBuffer;

public interface SequencedEventHandler extends EventHandler<SequencedEvent> {

    void onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length) throws Exception;

    @Override
    default void onEvent(SequencedEvent event, long sequence, boolean endOfBatch) throws Exception {
        onSequencedEvent(event.globalSequence, event.templateId, event.buffer, event.bufferLength);
    }
}
