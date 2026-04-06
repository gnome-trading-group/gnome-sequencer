package group.gnometrading.sequencer;

import com.lmax.disruptor.EventPoller;

/**
 * Polls a {@link SequencedRingBuffer} for available events without blocking.
 *
 * <p>Used by {@link group.gnometrading.concurrent.GnomeAgent} implementations to pull
 * events from a ring buffer in a {@code doWork()} loop rather than relying on
 * Disruptor-managed consumer threads.
 *
 * <p>Created via {@link SequencedRingBuffer#createPoller(SequencedEventHandler)}.
 * Zero-allocation on the hot path: the handler is stored at construction time.
 */
public final class SequencedPoller implements EventPoller.Handler<SequencedEvent> {

    private final EventPoller<SequencedEvent> poller;
    private final SequencedEventHandler handler;
    private int polledCount;

    SequencedPoller(EventPoller<SequencedEvent> poller, SequencedEventHandler handler) {
        this.poller = poller;
        this.handler = handler;
    }

    /**
     * Polls for available events and dispatches them to the configured handler.
     *
     * @return the number of events processed
     */
    public int poll() throws Exception {
        this.polledCount = 0;
        this.poller.poll(this);
        return this.polledCount;
    }

    @Override
    public boolean onEvent(SequencedEvent event, long sequence, boolean endOfBatch) throws Exception {
        this.handler.onSequencedEvent(event.globalSequence, event.templateId, event.buffer, event.bufferLength);
        this.polledCount++;
        return true;
    }
}
