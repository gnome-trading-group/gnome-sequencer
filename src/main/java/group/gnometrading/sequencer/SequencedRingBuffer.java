package group.gnometrading.sequencer;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import group.gnometrading.schemas.SbeMessage;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Wraps a LMAX Disruptor ring buffer with automatic global sequence assignment.
 *
 * <p>Producers call {@link #claim()} to get the pre-allocated schema flyweight,
 * write into it using the typed SBE encoder, then call {@link #publish()} to assign
 * a global sequence number and make the event visible to consumers.
 *
 * <p>All operations are zero-allocation. The schema flyweight is pre-allocated and
 * reused on every claim/publish cycle. Safe because each ring buffer uses
 * {@link ProducerType#SINGLE} — only one claim may be outstanding at a time.
 *
 * @param <T> the schema type produced by this ring buffer
 */
public final class SequencedRingBuffer<T extends SbeMessage> {

    public static final int DEFAULT_SIZE = 1 << 10;

    private final Disruptor<SequencedEvent> disruptor;
    private final RingBuffer<SequencedEvent> ringBuffer;
    private final GlobalSequence globalSequence;

    // Pre-allocated schema flyweight — returned by claim(), reused each cycle.
    private final T schema;

    // State for the currently claimed slot, held between claim() and publish().
    private SequencedEvent claimedEvent;
    private long claimedDisruptorSequence;

    public SequencedRingBuffer(EventFactory<T> schemaFactory, GlobalSequence globalSequence) {
        this(schemaFactory, globalSequence, DEFAULT_SIZE);
    }

    public SequencedRingBuffer(EventFactory<T> schemaFactory, GlobalSequence globalSequence, int size) {
        this.globalSequence = globalSequence;
        this.schema = schemaFactory.newInstance();
        this.disruptor = new Disruptor<>(
                SequencedEvent.FACTORY,
                size,
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE,
                new YieldingWaitStrategy());
        this.ringBuffer = this.disruptor.getRingBuffer();
    }

    /**
     * Claims the next slot in the ring buffer and returns the pre-allocated schema flyweight.
     *
     * <p>The caller writes event data into the returned schema using its SBE encoder.
     * Must be followed by a call to {@link #publish()}.
     */
    public T claim() {
        this.claimedDisruptorSequence = ringBuffer.next();
        this.claimedEvent = ringBuffer.get(this.claimedDisruptorSequence);
        return this.schema;
    }

    /**
     * Assigns a global sequence number, copies schema bytes into the ring buffer slot,
     * and publishes the event to downstream consumers.
     *
     * <p>Must be called after {@link #claim()}.
     */
    public void publish() {
        this.claimedEvent.globalSequence = globalSequence.next();
        this.claimedEvent.templateId = this.schema.messageHeaderDecoder.templateId();
        this.claimedEvent.bufferLength = this.schema.totalMessageSize();
        this.claimedEvent.buffer.putBytes(0, this.schema.buffer, 0, this.schema.totalMessageSize());
        this.ringBuffer.publish(this.claimedDisruptorSequence);
    }

    /**
     * Registers typed event handlers as consumers on this ring buffer.
     */
    public void handleEventsWith(SequencedEventHandler... handlers) {
        this.disruptor.handleEventsWith(handlers);
    }

    /**
     * Registers a raw {@link EventHandler} (e.g., {@link JournalWriter}) as a consumer.
     *
     * <p>Use this overload for handlers that operate directly on {@link SequencedEvent}
     * without the typed unwrapping provided by {@link SequencedEventHandler}.
     */
    public void addHandler(EventHandler<SequencedEvent> handler) {
        this.disruptor.handleEventsWith(handler);
    }

    /**
     * Creates a {@link SequencedPoller} for polling this ring buffer from a
     * {@link group.gnometrading.concurrent.GnomeAgent} doWork() loop.
     *
     * <p>Must be called before {@link #start()}. The handler is stored at construction
     * time for zero-allocation polling.
     */
    public SequencedPoller createPoller(SequencedEventHandler handler) {
        EventPoller<SequencedEvent> poller = this.ringBuffer.newPoller();
        this.ringBuffer.addGatingSequences(poller.getSequence());
        return new SequencedPoller(poller, handler);
    }

    /**
     * Publishes raw bytes from any SBE message into the ring buffer without using the
     * typed flyweight returned by {@link #claim()}.
     *
     * <p>Used when the caller owns the SBE message object (e.g., the OMS agent writing
     * Order/CancelOrder/ModifyOrder to the outbound ring buffer).
     */
    public void publishRaw(UnsafeBuffer src, int templateId, int length) {
        long seq = this.ringBuffer.next();
        SequencedEvent event = this.ringBuffer.get(seq);
        event.globalSequence = this.globalSequence.next();
        event.templateId = templateId;
        event.bufferLength = length;
        event.buffer.putBytes(0, src, 0, length);
        this.ringBuffer.publish(seq);
    }

    /**
     * Starts the Disruptor. Must be called after all handlers are registered.
     */
    public void start() {
        this.disruptor.start();
    }

    /**
     * Shuts down the Disruptor.
     */
    public void shutdown() {
        this.disruptor.shutdown();
    }
}
