package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournalRoundTripTest {

    private static final long FILE_SIZE = 1024L * 1024L;

    @TempDir
    Path tempDir;

    @Test
    void writeThenReadProducesSameEventsInOrder() throws IOException, InterruptedException {
        Path journalPath = tempDir.resolve("test.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);

        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, globalSequence);

        int eventCount = 10;
        CountDownLatch latch = new CountDownLatch(eventCount);

        ringBuffer.addHandler(writer);
        ringBuffer.handleEventsWith((gSeq, templateId, buffer, length) -> latch.countDown());
        ringBuffer.start();

        for (int i = 0; i < eventCount; i++) {
            ringBuffer.claim();
            ringBuffer.publish();
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for events");
        writer.close();
        ringBuffer.shutdown();

        // Read back and verify
        AtomicInteger readCount = new AtomicInteger(0);
        AtomicLong lastSeq = new AtomicLong(0);

        JournalReader reader = new JournalReader(journalPath);
        reader.readAll((gSeq, templateId, buffer, length) -> {
            readCount.incrementAndGet();
            lastSeq.set(gSeq);
            assertEquals(Mbp10Decoder.TEMPLATE_ID, templateId);
        });
        reader.close();

        assertEquals(eventCount, readCount.get(), "Should have read all events");
        assertEquals(eventCount, lastSeq.get(), "Last sequence should match event count");
        assertEquals(eventCount, reader.lastReadSequence());
    }

    @Test
    void reorderBufferHandlesOutOfOrderArrival() throws IOException {
        Path journalPath = tempDir.resolve("reorder.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);

        // Simulate events arriving out of global sequence order:
        // seq=2 arrives before seq=1
        SequencedEvent eventTwo = new SequencedEvent();
        eventTwo.globalSequence = 2;
        eventTwo.templateId = Mbp10Decoder.TEMPLATE_ID;
        eventTwo.bufferLength = 0;

        SequencedEvent eventOne = new SequencedEvent();
        eventOne.globalSequence = 1;
        eventOne.templateId = Mbp10Decoder.TEMPLATE_ID;
        eventOne.bufferLength = 0;

        // seq=2 arrives first — should be buffered, not flushed
        writer.onEvent(eventTwo, 0, false);
        assertEquals(0, writer.lastFlushedSequence(), "Should not flush seq=2 before seq=1");

        // seq=1 arrives — both should now flush in order
        writer.onEvent(eventOne, 0, false);
        assertEquals(2, writer.lastFlushedSequence(), "Should flush seq=1 and seq=2");

        writer.close();

        // Read back and verify order
        AtomicLong firstSeq = new AtomicLong(0);
        AtomicLong secondSeq = new AtomicLong(0);
        AtomicInteger callCount = new AtomicInteger(0);

        JournalReader reader = new JournalReader(journalPath);
        reader.readAll((gSeq, templateId, buffer, length) -> {
            int callIdx = callCount.getAndIncrement();
            if (callIdx == 0) {
                firstSeq.set(gSeq);
            } else {
                secondSeq.set(gSeq);
            }
        });
        reader.close();

        assertEquals(2, callCount.get());
        assertEquals(1L, firstSeq.get());
        assertEquals(2L, secondSeq.get());
    }

    @Test
    void journalFileExistsAfterWrite() throws IOException {
        Path journalPath = tempDir.resolve("exists.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);
        writer.close();
        assertTrue(Files.exists(journalPath));
    }
}
