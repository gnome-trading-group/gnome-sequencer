package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Mbp10Schema;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class LappingPollerTest {

    private static void publish(final SequencedRingBuffer<Mbp10Schema> ring, final long sequence) {
        ring.claim().encoder.sequence(sequence);
        ring.publish();
    }

    private static long sequenceOf(final UnsafeBuffer buffer) {
        final Mbp10Schema schema = new Mbp10Schema();
        schema.buffer.putBytes(0, buffer, 0, schema.totalMessageSize());
        return schema.decoder.sequence();
    }

    @Test
    void readsEveryEventOnceInOrder() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), 8);
        final List<Long> seen = new ArrayList<>();
        final LappingPoller reader =
                ring.createLappingPoller((seq, templateId, buffer, length) -> seen.add(sequenceOf(buffer)));

        for (int i = 1; i <= 5; i++) {
            publish(ring, i);
        }
        assertEquals(5, reader.poll());
        assertEquals(0, reader.poll());
        publish(ring, 6);
        assertEquals(1, reader.poll());

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), seen);
        assertEquals(0, reader.lapped());
    }

    @Test
    void neverHoldsTheProducerBackAndSkipsWhatItWasLappedPast() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), 8);
        final List<Long> seen = new ArrayList<>();
        final LappingPoller reader =
                ring.createLappingPoller((seq, templateId, buffer, length) -> seen.add(sequenceOf(buffer)));

        // Twenty into a ring of eight with nobody gating it: a gating poller would have made publish wait at the ninth.
        for (int i = 1; i <= 20; i++) {
            assertTrue(ring.tryPublishRaw(encoded(i), 0, templateId(), length()));
        }
        reader.poll();

        assertEquals(List.of(13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L), seen, "the last ring's worth");
        assertEquals(12, reader.lapped());
    }

    @Test
    void aMessageTooBigForASlotIsRefusedBeforeASlotIsClaimed() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), 8);
        final SequencedPoller poller = ring.createPoller((seq, templateId, buffer, length) -> {});
        final UnsafeBuffer big = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE + 1]);

        assertThrows(IllegalArgumentException.class, () -> ring.tryPublishRaw(big, 0, 1, big.capacity()));
        assertThrows(IllegalArgumentException.class, () -> ring.publishRaw(big, 0, 1, big.capacity()));
        publish(ring, 1);
        assertEquals(1, poller.poll(), "no slot left claimed and unpublished");
    }

    private static UnsafeBuffer encoded(final long sequence) {
        final Mbp10Schema schema = new Mbp10Schema();
        schema.encoder.sequence(sequence);
        return new UnsafeBuffer(schema.buffer, 0, schema.totalMessageSize());
    }

    private static int templateId() {
        return new Mbp10Schema().templateId();
    }

    private static int length() {
        return new Mbp10Schema().totalMessageSize();
    }
}
