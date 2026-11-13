package group.gnometrading.sequencer;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.utils.SampledEpochNanoClock;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Wraps a LMAX Disruptor {@link RingBuffer} with automatic global sequence assignment.
 *
 * <p>Producers call {@link #claim()} to get the pre-allocated schema flyweight,
 * write into it using the typed SBE encoder, then call {@link #publish()} to assign
 * a global sequence number and make the event visible to consumers.
 *
 * <p>Every publish stamps the event with the ring's clock ({@link SequencedEvent#timestampNanos}), which pollers hand
 * on ({@link SequencedPoller#timestampNanos()}, {@link LappingPoller#timestampNanos()}) and journals record. The
 * system clock ({@link SampledEpochNanoClock}, allocation-free) unless a clock is given: a simulation passes its own.
 *
 * <p>All operations are zero-allocation. The schema flyweight is pre-allocated and
 * reused on every claim/publish cycle. Safe because each ring buffer has a single
 * producer — only one claim may be outstanding at a time.
 *
 * <p>Consumers only read through {@link #createPoller}, from a
 * {@link group.gnometrading.concurrent.GnomeAgent} whose idle policy decides how it waits.
 * There are no buffer-owned consumer threads: a Disruptor-managed thread spins or yields under
 * its wait strategy forever, which pins a core per buffer and gets CPU-quota containers
 * throttled.
 *
 * @param <T> the schema type produced by this ring buffer
 */
public final class SequencedRingBuffer<T extends SbeMessage> {

    private static final VarHandle FULL_WAITS;

    static {
        try {
            FULL_WAITS = MethodHandles.lookup().findVarHandle(SequencedRingBuffer.class, "fullWaits", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static final int DEFAULT_SIZE = 1 << 10;

    private final RingBuffer<SequencedEvent> ringBuffer;
    private final GlobalSequence globalSequence;
    private final EpochNanoClock clock;

    // Pre-allocated schema flyweight — returned by claim(), reused each cycle.
    private final T schema;

    // State for the currently claimed slot, held between claim() and publish().
    private SequencedEvent claimedEvent;
    private long claimedDisruptorSequence;
    private long fullWaits;

    private volatile boolean stopped;

    public SequencedRingBuffer(EventFactory<T> schemaFactory, GlobalSequence globalSequence) {
        this(schemaFactory, globalSequence, DEFAULT_SIZE);
    }

    public SequencedRingBuffer(EventFactory<T> schemaFactory, GlobalSequence globalSequence, int size) {
        this(schemaFactory, globalSequence, size, Producers.ONE_THREAD);
    }

    public SequencedRingBuffer(
            EventFactory<T> schemaFactory, GlobalSequence globalSequence, int size, EpochNanoClock clock) {
        this(schemaFactory, globalSequence, size, Producers.ONE_THREAD, clock);
    }

    /**
     * Who publishes. A ring's claim and publish are never safe to interleave, so producers always publish one at a
     * time; the choice is whether that is always the same thread.
     */
    public enum Producers {
        /** One thread publishes for the ring's whole life. */
        ONE_THREAD,
        /**
         * Producers take turns, each handing over to the next with a happens-before edge (one stops, then the next
         * starts): a feed's markets move between connections, each read by its own thread.
         */
        TAKING_TURNS
    }

    public SequencedRingBuffer(
            EventFactory<T> schemaFactory, GlobalSequence globalSequence, int size, Producers producers) {
        this(schemaFactory, globalSequence, size, producers, SampledEpochNanoClock.INSTANCE);
    }

    public SequencedRingBuffer(
            EventFactory<T> schemaFactory,
            GlobalSequence globalSequence,
            int size,
            Producers producers,
            EpochNanoClock clock) {
        this.globalSequence = globalSequence;
        this.clock = clock;
        this.schema = schemaFactory.newInstance();
        // Pollers never wait on the strategy. Sleeping's signalAllWhenBlocking is a no-op, so publish()
        // pays nothing for it, unlike BlockingWaitStrategy's lock + notifyAll.
        this.ringBuffer = producers == Producers.ONE_THREAD
                ? RingBuffer.createSingleProducer(SequencedEvent.FACTORY, size, new SleepingWaitStrategy())
                : RingBuffer.createMultiProducer(SequencedEvent.FACTORY, size, new SleepingWaitStrategy());
        globalSequence.register(this);
    }

    /**
     * Claims the next slot in the ring buffer and returns the pre-allocated schema flyweight.
     *
     * <p>The caller writes event data into the returned schema using its SBE encoder.
     * Must be followed by a call to {@link #publish()}. Waits while the ring is full.
     *
     * @throws RingBufferStoppedException if the ring is stopped, or the thread interrupted, while waiting for room
     */
    public T claim() {
        this.claimedDisruptorSequence = nextSequence();
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
        this.claimedEvent.timestampNanos = clock.nanoTime();
        this.claimedEvent.templateId = this.schema.templateId();
        this.claimedEvent.bufferLength = this.schema.totalMessageSize();
        this.claimedEvent.buffer.putBytes(0, this.schema.buffer, 0, this.schema.totalMessageSize());
        this.ringBuffer.publish(this.claimedDisruptorSequence);
    }

    /**
     * Creates a {@link LappingPoller}: a reader that never holds the producer back and is lapped instead when it falls a
     * whole ring behind. Like {@link #createPoller}, it sees only events published after it is created.
     */
    public LappingPoller createLappingPoller(SequencedEventHandler handler) {
        return new LappingPoller(this.ringBuffer, (globalSequence, templateId, buffer, length) -> {
            handler.onSequencedEvent(globalSequence, templateId, buffer, length);
            return true;
        });
    }

    /**
     * As {@link #createLappingPoller}, but the handler may hold an event it can't take yet, which the next poll hands it
     * again; holding never slows the producer, so a reader that holds too long is lapped past the event.
     */
    public LappingPoller createControlledLappingPoller(ControlledSequencedEventHandler handler) {
        return new LappingPoller(this.ringBuffer, handler);
    }

    /**
     * Creates a {@link SequencedPoller} for polling this ring buffer from a
     * {@link group.gnometrading.concurrent.GnomeAgent} doWork() loop.
     *
     * <p>Must be called before the producer first publishes: the poller only sees events published
     * after it is created, and only gates the producer from then on. The handler is stored at
     * construction time for zero-allocation polling.
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
     * Order/CancelOrder/ModifyOrder to the outbound ring buffer). Waits while the ring is full.
     *
     * @throws RingBufferStoppedException if the ring is stopped, or the thread interrupted, while waiting for room
     */
    public void publishRaw(UnsafeBuffer src, int templateId, int length) {
        publishRaw(src, 0, templateId, length);
    }

    /**
     * As {@link #publishRaw(UnsafeBuffer, int, int)}, copying {@code length} bytes from {@code offset}: for a message
     * that sits inside a larger buffer, such as an Aeron fragment, so it needn't be copied to offset 0 first.
     */
    public void publishRaw(DirectBuffer src, int offset, int templateId, int length) {
        checkLength(length);
        long seq = nextSequence();
        SequencedEvent event = this.ringBuffer.get(seq);
        event.globalSequence = this.globalSequence.next();
        event.timestampNanos = this.clock.nanoTime();
        event.templateId = templateId;
        event.bufferLength = length;
        event.buffer.putBytes(0, src, offset, length);
        this.ringBuffer.publish(seq);
    }

    /**
     * As {@link #publishRaw(DirectBuffer, int, int, int)}, but never waits: returns false, publishing nothing, while the
     * slowest poller is a whole ring behind. For a producer that must not block, such as an Aeron poller that leaves the
     * message in its log and retries. Producers must publish one at a time ({@link Producers}): with room seen, the slot
     * is this caller's.
     */
    public boolean tryPublishRaw(DirectBuffer src, int offset, int templateId, int length) {
        checkLength(length);
        if (!this.ringBuffer.hasAvailableCapacity(1)) {
            return false;
        }
        long seq = this.ringBuffer.next();
        SequencedEvent event = this.ringBuffer.get(seq);
        event.globalSequence = this.globalSequence.next();
        event.timestampNanos = this.clock.nanoTime();
        event.templateId = templateId;
        event.bufferLength = length;
        event.buffer.putBytes(0, src, offset, length);
        this.ringBuffer.publish(seq);
        return true;
    }

    /** Before claiming a slot: a message that can't fit would leave the claimed slot unpublished for good. */
    private static void checkLength(int length) {
        if (length < 0 || length > SequencedEvent.MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("A message of " + length + " bytes doesn't fit a "
                    + SequencedEvent.MAX_MESSAGE_SIZE + "-byte slot");
        }
    }

    /**
     * Releases a producer waiting for room, for good: a wait that would otherwise last forever, on a consumer that has
     * stopped, ends in {@link RingBufferStoppedException}. A claim that finds room is unaffected. For the producer's
     * own shutdown; the ring isn't used after.
     */
    /** Events published since the ring was made. Any thread. */
    public long published() {
        return this.ringBuffer.getCursor() + 1;
    }

    /**
     * Events published that the slowest gating reader (a {@link #createPoller} reader; lapping readers never gate) has
     * not yet taken: how far behind the ring's consumers are. Any thread.
     */
    public long backlog() {
        return this.ringBuffer.getCursor() - this.ringBuffer.getMinimumGatingSequence();
    }

    /** How many events the ring holds. */
    public int capacity() {
        return this.ringBuffer.getBufferSize();
    }

    /** Publishes that found the ring full and waited for a reader: the producer held back. Any thread. */
    public long fullWaits() {
        return (long) FULL_WAITS.getAcquire(this);
    }

    public void stop() {
        this.stopped = true;
    }

    /**
     * The next slot, waiting while the slowest poller is a whole ring behind. Disruptor's own wait can't be ended: it
     * parks in a loop that ignores both interrupts and shutdown, so a producer on a consumer that stopped would hang.
     */
    private long nextSequence() {
        if (!this.ringBuffer.hasAvailableCapacity(1)) {
            // A count for readers on other threads, seen through the release store. With several producers two may
            // count at once and one be lost: it's a measure, not a sequence.
            FULL_WAITS.setRelease(this, this.fullWaits + 1);
        }
        while (!this.ringBuffer.hasAvailableCapacity(1)) {
            if (this.stopped || Thread.currentThread().isInterrupted()) {
                throw RingBufferStoppedException.INSTANCE;
            }
            LockSupport.parkNanos(1L);
        }
        // Single producer: with room seen, next() returns without waiting.
        return this.ringBuffer.next();
    }
}
