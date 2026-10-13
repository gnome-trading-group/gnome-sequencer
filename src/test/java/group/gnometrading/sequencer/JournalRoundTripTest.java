package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.MessageHeaderDecoder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournalRoundTripTest {

    private static final long FILE_SIZE = 1024L * 1024L;
    private static final UnsafeBuffer EMPTY = new UnsafeBuffer(new byte[0]);

    @TempDir
    Path tempDir;

    @Test
    void writeThenReadProducesSameEventsInOrder() throws Exception {
        Path journalPath = tempDir.resolve("test.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);

        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> ringBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, globalSequence);
        SequencedPoller poller = ringBuffer.createPoller(writer.source(7));

        int eventCount = 10;
        for (int i = 0; i < eventCount; i++) {
            ringBuffer.claim();
            ringBuffer.publish();
        }
        assertEquals(eventCount, poller.poll());
        writer.close();

        AtomicInteger readCount = new AtomicInteger(0);
        AtomicLong lastSeq = new AtomicLong(0);

        JournalReader reader = new JournalReader(journalPath);
        reader.readAll((gSeq, sourceId, templateId, buffer, length) -> {
            readCount.incrementAndGet();
            lastSeq.set(gSeq);
            assertEquals(7, sourceId);
            assertEquals(Mbp10Decoder.TEMPLATE_ID, templateId);
        });
        reader.close();

        assertEquals(eventCount, readCount.get(), "Should have read all events");
        assertEquals(eventCount, lastSeq.get(), "Last sequence should match event count");
        assertEquals(eventCount, reader.lastReadSequence());
    }

    @Test
    void buffersSharingASequenceAreJournaledInGlobalOrderWithTheirSource() throws Exception {
        Path journalPath = tempDir.resolve("interleaved.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);

        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> marketData = new SequencedRingBuffer<>(Mbp10Schema::new, globalSequence);
        SequencedRingBuffer<Intent> intents = new SequencedRingBuffer<>(Intent::new, globalSequence);
        SequencedPoller marketDataPoller = marketData.createPoller(writer.source(1));
        SequencedPoller intentPoller = intents.createPoller(writer.source(2));

        marketData.claim();
        marketData.publish();
        intents.claim();
        intents.publish();
        marketData.claim();
        marketData.publish();
        intents.claim();
        intents.publish();

        // Draining one buffer fully before the other delivers 1, 3 then 2, 4.
        marketDataPoller.poll();
        assertEquals(1, writer.lastFlushedSequence());
        intentPoller.poll();
        assertEquals(4, writer.lastFlushedSequence());
        writer.close();

        List<Long> sequences = new ArrayList<>();
        List<Integer> sources = new ArrayList<>();
        List<Integer> templates = new ArrayList<>();
        JournalReader reader = new JournalReader(journalPath);
        reader.readAll((gSeq, sourceId, templateId, buffer, length) -> {
            sequences.add(gSeq);
            sources.add(sourceId);
            templates.add(templateId);
        });
        reader.close();

        assertEquals(List.of(1L, 2L, 3L, 4L), sequences);
        assertEquals(List.of(1, 2, 1, 2), sources);
        assertEquals(
                List.of(
                        Mbp10Decoder.TEMPLATE_ID,
                        IntentDecoder.TEMPLATE_ID,
                        Mbp10Decoder.TEMPLATE_ID,
                        IntentDecoder.TEMPLATE_ID),
                templates);
    }

    @Test
    void reorderBufferHandlesOutOfOrderArrival() throws IOException {
        Path journalPath = tempDir.resolve("reorder.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);

        writer.append(0, 2, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        assertEquals(0, writer.lastFlushedSequence(), "Should not flush seq=2 before seq=1");

        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        assertEquals(2, writer.lastFlushedSequence(), "Should flush seq=1 and seq=2");

        writer.close();

        List<Long> sequences = new ArrayList<>();
        JournalReader reader = new JournalReader(journalPath);
        reader.readAll((gSeq, sourceId, templateId, buffer, length) -> sequences.add(gSeq));
        reader.close();

        assertEquals(List.of(1L, 2L), sequences);
    }

    @Test
    void unjournaledBufferOnTheSequenceFailsLoudlyInsteadOfStalling() throws Exception {
        JournalWriter writer = new JournalWriter(tempDir.resolve("gap.journal"), FILE_SIZE);

        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Intent> journaled = new SequencedRingBuffer<>(Intent::new, globalSequence);
        SequencedRingBuffer<Intent> unjournaled = new SequencedRingBuffer<>(Intent::new, globalSequence);
        SequencedPoller poller = journaled.createPoller(writer.source(1));

        journaled.claim();
        journaled.publish();
        unjournaled.claim();
        unjournaled.publish();
        poller.poll();
        assertEquals(1, writer.lastFlushedSequence());

        // Sequence 2 never reaches the journal; everything after it waits until the window overflows.
        IllegalStateException gap = assertThrows(IllegalStateException.class, () -> {
            for (int i = 0; i < SequencedRingBuffer.DEFAULT_SIZE; i++) {
                journaled.claim();
                journaled.publish();
                poller.poll();
            }
        });
        assertTrue(gap.getMessage().contains("waiting for global sequence 2"), gap.getMessage());
        writer.close();
    }

    @Test
    void appendingAnAlreadyJournaledSequenceThrows() throws IOException {
        JournalWriter writer = new JournalWriter(tempDir.resolve("duplicate.journal"), FILE_SIZE);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);

        assertThrows(IllegalStateException.class, () -> writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0));
        writer.close();
    }

    @Test
    void recordsFromAnOlderSchemaVersionAreReplayedAtTheCurrentOne() throws IOException {
        Path journalPath = tempDir.resolve("old-version.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);

        // An Intent journaled by schema v0, with a size beyond the old uint32 signed range.
        UnsafeBuffer oldIntent = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        new group.gnometrading.schemas.v0.IntentEncoder()
                .wrapAndApplyHeader(oldIntent, 0, new group.gnometrading.schemas.v0.MessageHeaderEncoder())
                .bidPrice(55L)
                .bidSize(4_000_000_000L);
        int oldIntentLength =
                MessageHeaderDecoder.ENCODED_LENGTH + group.gnometrading.schemas.v0.IntentEncoder.BLOCK_LENGTH;
        writer.append(0, 1, IntentDecoder.TEMPLATE_ID, oldIntent, oldIntentLength);
        writer.close();

        AtomicLong bidSize = new AtomicLong();
        AtomicInteger version = new AtomicInteger(-1);
        JournalReader reader = new JournalReader(journalPath);
        reader.readAll((gSeq, sourceId, templateId, buffer, length) -> {
            IntentDecoder decoder = new IntentDecoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
            version.set(decoder.sbeSchemaVersion());
            bidSize.set(decoder.bidSize());
            assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + IntentDecoder.BLOCK_LENGTH, length);
        });
        reader.close();

        assertEquals(IntentDecoder.SCHEMA_VERSION, version.get());
        assertEquals(4_000_000_000L, bidSize.get());
    }

    @Test
    void journalFileExistsAfterWrite() throws IOException {
        Path journalPath = tempDir.resolve("exists.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE);
        writer.close();
        assertTrue(Files.exists(journalPath));
    }
}
