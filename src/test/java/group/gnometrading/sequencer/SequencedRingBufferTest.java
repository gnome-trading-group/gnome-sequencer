package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.book.Mbp10Decoder;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class SequencedRingBufferTest {

    @Test
    void claimReturnsPreallocatedSchemaFlyweight() {
        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, globalSequence);

        Mbp10Schema firstClaim = ringBuffer.claim();
        assertNotNull(firstClaim);

        // Same object returned on every claim (pre-allocated flyweight)
        ringBuffer.publish();
        Mbp10Schema secondClaim = ringBuffer.claim();
        assertEquals(firstClaim, secondClaim);
    }

    @Test
    void publishAssignsMonotonicGlobalSequence() throws Exception {
        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, globalSequence);

        int eventCount = 5;
        long[] receivedGlobalSequences = new long[eventCount];
        int[] receivedTemplateIds = new int[eventCount];
        SequencedPoller poller = ringBuffer.createPoller((gSeq, templateId, buffer, length) -> {
            int idx = (int) (gSeq - 1);
            receivedGlobalSequences[idx] = gSeq;
            receivedTemplateIds[idx] = templateId;
        });

        for (int i = 0; i < eventCount; i++) {
            Mbp10Schema schema = ringBuffer.claim();
            schema.encoder.streamId(i);
            ringBuffer.publish();
        }

        assertEquals(eventCount, poller.poll());

        for (int i = 0; i < eventCount; i++) {
            assertEquals(i + 1, receivedGlobalSequences[i], "Wrong global sequence at index " + i);
            assertEquals(Mbp10Decoder.TEMPLATE_ID, receivedTemplateIds[i]);
        }
    }

    @Test
    void publishRawCopiesFromAnOffsetInALargerBuffer() throws Exception {
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        Mbp10Schema message = new Mbp10Schema();
        message.encoder.streamId(42);
        int length = message.totalMessageSize();
        int offset = 37;
        UnsafeBuffer frame = new UnsafeBuffer(new byte[offset + length + 11]);
        frame.putBytes(offset, message.buffer, 0, length);
        Mbp10Schema received = new Mbp10Schema();
        int[] receivedLength = new int[1];
        SequencedPoller poller = ringBuffer.createPoller((gSeq, templateId, buffer, len) -> {
            received.buffer.putBytes(0, buffer, 0, len);
            receivedLength[0] = len;
        });

        ringBuffer.publishRaw(frame, offset, Mbp10Decoder.TEMPLATE_ID, length);

        assertEquals(1, poller.poll());
        assertEquals(length, receivedLength[0]);
        assertEquals(42, received.decoder.streamId());
    }

    @Test
    void tryPublishRawRefusesWhileTheRingIsFullAndTakesAgainOnceItDrains() throws Exception {
        SequencedRingBuffer<Mbp10Schema> ringBuffer =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), 4);
        SequencedPoller poller = ringBuffer.createPoller((gSeq, templateId, buffer, len) -> {});
        UnsafeBuffer frame = new UnsafeBuffer(new byte[64]);

        for (int i = 0; i < 4; i++) {
            assertTrue(ringBuffer.tryPublishRaw(frame, 0, Mbp10Decoder.TEMPLATE_ID, 64));
        }
        assertFalse(ringBuffer.tryPublishRaw(frame, 0, Mbp10Decoder.TEMPLATE_ID, 64));

        assertEquals(4, poller.poll());
        assertTrue(ringBuffer.tryPublishRaw(frame, 0, Mbp10Decoder.TEMPLATE_ID, 64));
    }

    @Test
    void pollReturnsZeroWhenNothingWasPublished() throws Exception {
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        SequencedPoller poller = ringBuffer.createPoller((gSeq, templateId, buffer, length) -> {});

        assertEquals(0, poller.poll());
    }

    @Test
    void everyPollerSeesEveryEvent() throws Exception {
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        List<Long> first = new ArrayList<>();
        List<Long> second = new ArrayList<>();
        SequencedPoller firstPoller = ringBuffer.createPoller((gSeq, templateId, buffer, length) -> first.add(gSeq));
        SequencedPoller secondPoller = ringBuffer.createPoller((gSeq, templateId, buffer, length) -> second.add(gSeq));

        for (int i = 0; i < 3; i++) {
            ringBuffer.claim();
            ringBuffer.publish();
        }
        firstPoller.poll();
        secondPoller.poll();

        assertEquals(List.of(1L, 2L, 3L), first);
        assertEquals(List.of(1L, 2L, 3L), second);
    }

    @Test
    void producerWrapsTheRingOnlyAfterThePollerHasConsumed() throws Exception {
        int size = 4;
        SequencedRingBuffer<Mbp10Schema> ringBuffer =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), size);
        List<Long> received = new ArrayList<>();
        SequencedPoller poller = ringBuffer.createPoller((gSeq, templateId, buffer, length) -> received.add(gSeq));

        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < size; i++) {
                ringBuffer.claim();
                ringBuffer.publish();
            }
            assertEquals(size, poller.poll());
        }

        assertEquals(3 * size, received.size());
        for (int i = 0; i < received.size(); i++) {
            assertEquals(i + 1, received.get(i));
        }
    }

    /** A feed's market moves between connections, each read on its own thread; they take turns, never interleave. */
    @Test
    void producersOnDifferentThreadsCanTakeTurns() throws Exception {
        SequencedRingBuffer<Mbp10Schema> ring = new SequencedRingBuffer<>(
                Mbp10Schema::new, new GlobalSequence(), 64, SequencedRingBuffer.Producers.TAKING_TURNS);
        List<Long> seen = new ArrayList<>();
        SequencedPoller poller = ring.createPoller((seq, templateId, buffer, length) -> seen.add(seq));
        for (int turn = 0; turn < 3; turn++) {
            Thread producer = new Thread(() -> {
                for (int i = 0; i < 5; i++) {
                    ring.claim();
                    ring.publish();
                }
            });
            producer.start();
            producer.join();
        }
        while (poller.poll() > 0) {}

        assertEquals(15, seen.size());
        for (int i = 1; i < seen.size(); i++) {
            assertEquals(seen.get(i - 1) + 1, seen.get(i));
        }
    }
}
