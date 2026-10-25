package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.book.Mbp10Decoder;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/** A producer waiting on a full ring, behind a consumer that never polls, must be releasable. */
class SequencedRingBufferStopTest {

    private static final int SIZE = 8;

    @Test
    void stopReleasesAClaimWaitingForRoom() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring = ring();
        final Blocked blocked = fillThen(ring, SequencedRingBuffer::claim);

        ring.stop();

        assertSame(RingBufferStoppedException.INSTANCE, blocked.awaitThrown());
    }

    @Test
    void stopReleasesAPublishRawWaitingForRoom() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring = ring();
        final Mbp10Schema message = new Mbp10Schema();
        final Blocked blocked =
                fillThen(ring, r -> r.publishRaw(message.buffer, Mbp10Decoder.TEMPLATE_ID, message.totalMessageSize()));

        ring.stop();

        assertSame(RingBufferStoppedException.INSTANCE, blocked.awaitThrown());
    }

    @Test
    void anInterruptReleasesAClaimWaitingForRoom() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring = ring();
        final Blocked blocked = fillThen(ring, SequencedRingBuffer::claim);

        blocked.thread.interrupt();

        assertSame(RingBufferStoppedException.INSTANCE, blocked.awaitThrown());
    }

    /** Backpressure is kept: a full ring with a live consumer waits for it, and the claim then succeeds. */
    @Test
    void aClaimOnAFullRingWaitsForTheConsumer() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), SIZE);
        final SequencedPoller poller = ring.createPoller((seq, templateId, buffer, length) -> {});
        final Blocked blocked = fillThen(ring, SequencedRingBuffer::claim);

        poller.poll();

        assertTrue(blocked.done.await(5, TimeUnit.SECONDS));
        assertNull(blocked.thrown.get());
    }

    @Test
    void stopDoesNotAffectAClaimThatFindsRoom() {
        final SequencedRingBuffer<Mbp10Schema> ring = ring();
        ring.stop();

        assertNotNull(ring.claim());
        ring.publish();
    }

    @Test
    void claimingWithRoomDoesNotAllocate() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        final SequencedPoller poller = ring.createPoller((seq, templateId, buffer, length) -> {});
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().getId();
        cycle(ring, poller, 50_000);
        final long before = threads.getThreadAllocatedBytes(threadId);
        cycle(ring, poller, 10_000);
        final long allocated = threads.getThreadAllocatedBytes(threadId) - before;
        // Measuring can itself allocate a little; anything per claim would be hundreds of kilobytes here.
        assertTrue(allocated < 2_048, "allocated " + allocated + " bytes over 10000 claims");
    }

    private static void cycle(final SequencedRingBuffer<Mbp10Schema> ring, final SequencedPoller poller, final int n)
            throws Exception {
        for (int i = 0; i < n; i++) {
            ring.claim();
            ring.publish();
            poller.poll();
        }
    }

    /** A small ring with a poller that never polls. */
    private static SequencedRingBuffer<Mbp10Schema> ring() {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), SIZE);
        ring.createPoller((seq, templateId, buffer, length) -> {});
        return ring;
    }

    /**
     * Fills the ring and then produces once more, on one thread (the ring has a single producer), which has to wait;
     * returns once it is waiting.
     */
    private static Blocked fillThen(
            final SequencedRingBuffer<Mbp10Schema> ring, final Consumer<SequencedRingBuffer<Mbp10Schema>> producer)
            throws Exception {
        final Blocked blocked = new Blocked();
        final CountDownLatch filled = new CountDownLatch(1);
        blocked.thread = new Thread(() -> {
            try {
                for (int i = 0; i < SIZE; i++) {
                    ring.claim();
                    ring.publish();
                }
                filled.countDown();
                producer.accept(ring);
            } catch (final Throwable t) {
                blocked.thrown.set(t);
            } finally {
                blocked.done.countDown();
            }
        });
        // A producer that is never released mustn't keep the test JVM alive.
        blocked.thread.setDaemon(true);
        blocked.thread.start();
        assertTrue(filled.await(5, TimeUnit.SECONDS));
        // Long enough to be parked in the wait, not still on its way in.
        assertFalse(blocked.done.await(100, TimeUnit.MILLISECONDS), "the producer is waiting for room");
        return blocked;
    }

    private static final class Blocked {
        Thread thread;
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicReference<Throwable> thrown = new AtomicReference<>();

        Throwable awaitThrown() throws InterruptedException {
            assertTrue(this.done.await(5, TimeUnit.SECONDS), "the producer was released");
            return this.thrown.get();
        }
    }
}
