package group.gnometrading.sequencer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared global sequence number generator for the sequenced event bus.
 *
 * <p>Each producer calls {@link #next()} to grab a monotonically increasing sequence number
 * before publishing to its ring buffer. A single instance is shared across all
 * {@link SequencedRingBuffer} instances in the system.
 *
 * <p>The underlying {@link AtomicLong#incrementAndGet()} is a single {@code LOCK XADD}
 * instruction (~5-10ns uncontended). Contention is negligible because the market data path
 * dominates and other producers (OMS actions, execution reports) are orders of magnitude
 * less frequent.
 */
public final class GlobalSequence {

    private final AtomicLong sequence = new AtomicLong(0);

    /**
     * Returns the next monotonically increasing global sequence number.
     *
     * @return the next sequence number
     */
    public long next() {
        return sequence.incrementAndGet();
    }

    /**
     * Returns the current global sequence number without incrementing.
     *
     * @return the current sequence number
     */
    public long current() {
        return sequence.get();
    }
}
