package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

        ringBuffer.shutdown();
    }

    @Test
    void publishAssignsMonotonicGlobalSequence() throws InterruptedException {
        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, globalSequence);

        int eventCount = 5;
        long[] receivedGlobalSequences = new long[eventCount];
        int[] receivedTemplateIds = new int[eventCount];
        CountDownLatch latch = new CountDownLatch(eventCount);

        ringBuffer.handleEventsWith((gSeq, templateId, buffer, length) -> {
            int idx = (int) (gSeq - 1);
            receivedGlobalSequences[idx] = gSeq;
            receivedTemplateIds[idx] = templateId;
            latch.countDown();
        });
        ringBuffer.start();

        for (int i = 0; i < eventCount; i++) {
            Mbp10Schema schema = ringBuffer.claim();
            schema.encoder.exchangeId((short) i);
            ringBuffer.publish();
        }

        latch.await(5, TimeUnit.SECONDS);

        for (int i = 0; i < eventCount; i++) {
            assertEquals(i + 1, receivedGlobalSequences[i], "Wrong global sequence at index " + i);
            assertEquals(Mbp10Decoder.TEMPLATE_ID, receivedTemplateIds[i]);
        }

        ringBuffer.shutdown();
    }
}
