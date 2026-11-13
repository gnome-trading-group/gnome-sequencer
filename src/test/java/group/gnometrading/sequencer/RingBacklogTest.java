package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Intent;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RingBacklogTest {

    private static void publish(SequencedRingBuffer<Intent> ring) {
        ring.claim();
        ring.publish();
    }

    @Test
    void theBacklogIsWhatTheSlowestGatingReaderHasNotTaken() throws Exception {
        SequencedRingBuffer<Intent> ring = new SequencedRingBuffer<>(Intent::new, new GlobalSequence(), 8);
        SequencedPoller fast = ring.createPoller((seq, template, buffer, length) -> {});
        SequencedPoller slow = ring.createPoller((seq, template, buffer, length) -> {});
        ring.createLappingPoller((seq, template, buffer, length) -> {});
        for (int i = 0; i < 5; i++) {
            publish(ring);
        }
        fast.poll();
        slow.poll(2);

        assertEquals(5, ring.published());
        assertEquals(3, ring.backlog(), "the slow reader's 3, not the fast one's 0; the lapping reader never gates");
        assertEquals(8, ring.capacity());
        assertEquals(0, ring.fullWaits());
    }

    @Test
    void aProducerThatFindsTheRingFullIsCounted() throws Exception {
        SequencedRingBuffer<Intent> ring = new SequencedRingBuffer<>(Intent::new, new GlobalSequence(), 4);
        SequencedPoller reader = ring.createPoller((seq, template, buffer, length) -> {});
        AtomicBoolean done = new AtomicBoolean();
        // The ring's one producer: four fill it, the fifth waits for the reader.
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                publish(ring);
            }
            done.set(true);
        });
        producer.start();
        while (ring.fullWaits() == 0) {
            Thread.onSpinWait();
        }
        assertEquals(4, ring.backlog());
        reader.poll();
        producer.join(5_000);
        assertTrue(done.get());
        assertEquals(1, ring.fullWaits());
        assertEquals(1, ring.backlog(), "the waiting event got in once the reader made room");
    }
}
