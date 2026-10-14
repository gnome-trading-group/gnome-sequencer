package group.gnometrading.sequencer;

import group.gnometrading.concurrent.AgentStoppedException;

/**
 * A producer was waiting for room in a {@link SequencedRingBuffer} that has been stopped, or its thread was
 * interrupted: the consumer it waited on will never make room, so the producer gives up. One shared instance, so
 * throwing it allocates nothing.
 */
public final class RingBufferStoppedException extends AgentStoppedException {

    static final RingBufferStoppedException INSTANCE = new RingBufferStoppedException();

    private RingBufferStoppedException() {
        super("Stopped while waiting for room in the ring buffer");
    }
}
