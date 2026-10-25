package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.book.Mbp10Decoder;
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

    private final List<JournalSegment> segments = new ArrayList<>();

    private static Path firstSegment(Path journal) {
        return journal.resolve(JournalWriter.segmentFileName(0));
    }

    @Test
    void writeThenReadProducesSameEventsInOrder() throws Exception {
        Path journalPath = tempDir.resolve("test.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);

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

        JournalReader reader = new JournalReader(firstSegment(journalPath));
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
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);

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
        JournalReader reader = new JournalReader(firstSegment(journalPath));
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
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);

        writer.append(0, 2, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        assertEquals(0, writer.lastFlushedSequence(), "Should not flush seq=2 before seq=1");

        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        assertEquals(2, writer.lastFlushedSequence(), "Should flush seq=1 and seq=2");

        writer.close();

        List<Long> sequences = new ArrayList<>();
        JournalReader reader = new JournalReader(firstSegment(journalPath));
        reader.readAll((gSeq, sourceId, templateId, buffer, length) -> sequences.add(gSeq));
        reader.close();

        assertEquals(List.of(1L, 2L), sequences);
    }

    @Test
    void unjournaledBufferOnTheSequenceFailsLoudlyInsteadOfStalling() throws Exception {
        JournalWriter writer = new JournalWriter(tempDir.resolve("gap.journal"), FILE_SIZE, segments::add);

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
        JournalWriter writer = new JournalWriter(tempDir.resolve("duplicate.journal"), FILE_SIZE, segments::add);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);

        assertThrows(IllegalStateException.class, () -> writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0));
        writer.close();
    }

    @Test
    void recordsFromAnOlderSchemaVersionAreReplayedAtTheCurrentOne() throws IOException {
        Path journalPath = tempDir.resolve("old-version.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);

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
        JournalReader reader = new JournalReader(firstSegment(journalPath));
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
    void closingHandsOnTheLastSegment() throws IOException {
        Path journalPath = tempDir.resolve("closed.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.append(0, 2, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.close();

        assertEquals(1, segments.size());
        JournalSegment segment = segments.get(0);
        assertEquals(0, segment.index());
        assertEquals(firstSegment(journalPath), segment.path());
        assertEquals(2 * JournalWriter.HEADER_SIZE, segment.writtenBytes());
        assertEquals(1, segment.firstSequence());
        assertEquals(2, segment.lastSequence());
    }

    @Test
    void sealingTrimsTheSegmentToItsRecordsAndMarksItDone() throws IOException {
        Path journalPath = tempDir.resolve("sealed.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.append(0, 2, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.close();

        JournalSegment segment = segments.get(0);
        segment.seal();

        assertEquals(journalPath.resolve(JournalWriter.sealedFileName(0)), segment.path());
        assertFalse(Files.exists(firstSegment(journalPath)));
        assertEquals(2L * JournalWriter.HEADER_SIZE, Files.size(segment.path()));
        List<Long> sequences = new ArrayList<>();
        JournalReader reader = new JournalReader(segment.path());
        reader.readAll((gSeq, sourceId, templateId, buffer, length) -> sequences.add(gSeq));
        reader.close();
        assertEquals(List.of(1L, 2L), sequences);
    }

    @Test
    void anEmptyJournalLeavesNoSegment() throws IOException {
        Path journalPath = tempDir.resolve("empty.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);
        writer.close();

        assertTrue(segments.isEmpty());
        assertFalse(Files.exists(firstSegment(journalPath)));
    }

    @Test
    void rollStartsTheNextSegmentAndReadingCarriesOnAcrossIt() throws IOException {
        Path journalPath = tempDir.resolve("rolled.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.append(0, 2, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.roll();
        writer.append(0, 3, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.close();

        assertEquals(2, segments.size());
        assertEquals(
                List.of(1L, 3L),
                List.of(segments.get(0).firstSequence(), segments.get(1).firstSequence()));
        assertEquals(
                journalPath.resolve(JournalWriter.segmentFileName(1)),
                segments.get(1).path());

        List<Long> sequences = new ArrayList<>();
        long previous = 0;
        for (JournalSegment segment : segments) {
            segment.seal();
            JournalReader reader = new JournalReader(segment.path(), previous);
            reader.readAll((gSeq, sourceId, templateId, buffer, length) -> sequences.add(gSeq));
            previous = reader.lastReadSequence();
            reader.close();
        }
        assertEquals(List.of(1L, 2L, 3L), sequences);
    }

    @Test
    void rollingAnEmptySegmentDoesNothing() throws IOException {
        JournalWriter writer = new JournalWriter(tempDir.resolve("idle.journal"), FILE_SIZE, segments::add);
        writer.roll();
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.roll();
        writer.roll();

        assertEquals(1, segments.size());
        assertEquals(0, writer.segmentWrittenBytes());
        writer.close();
        assertEquals(1, segments.size());
    }

    @Test
    void aRecordThatDoesNotFitStartsTheNextSegmentWhole() throws IOException {
        int recordBytes = JournalWriter.HEADER_SIZE + SequencedEvent.MAX_MESSAGE_SIZE;
        long segmentSize = recordBytes * 2L + recordBytes / 2;
        UnsafeBuffer payload = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        payload.setMemory(0, SequencedEvent.MAX_MESSAGE_SIZE, (byte) 7);
        Path journalPath = tempDir.resolve("full.journal");
        JournalWriter writer = new JournalWriter(journalPath, segmentSize, segments::add);
        for (int seq = 1; seq <= 5; seq++) {
            writer.append(0, seq, Mbp10Decoder.TEMPLATE_ID, payload, SequencedEvent.MAX_MESSAGE_SIZE);
        }
        writer.close();

        assertEquals(3, segments.size());
        assertEquals(
                List.of(2 * recordBytes, 2 * recordBytes, recordBytes),
                segments.stream().map(JournalSegment::writtenBytes).toList());

        List<Long> sequences = new ArrayList<>();
        long previous = 0;
        for (JournalSegment segment : segments) {
            segment.seal();
            JournalReader reader = new JournalReader(segment.path(), previous);
            reader.readAll((gSeq, sourceId, templateId, buffer, length) -> {
                sequences.add(gSeq);
                assertEquals(SequencedEvent.MAX_MESSAGE_SIZE, length);
                assertEquals(7, buffer.getByte(length - 1));
            });
            previous = reader.lastReadSequence();
            reader.close();
        }
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sequences);
    }

    @Test
    void aSegmentNotFollowingTheOneBeforeIsAGap() throws IOException {
        Path journalPath = tempDir.resolve("gapped.journal");
        JournalWriter writer = new JournalWriter(journalPath, FILE_SIZE, segments::add);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.roll();
        writer.append(0, 2, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.close();
        segments.forEach(JournalSegment::seal);

        JournalReader reader = new JournalReader(segments.get(1).path(), 0);
        assertThrows(IllegalStateException.class, () -> reader.readAll((gSeq, sourceId, templateId, b, l) -> {}));
    }

    @Test
    void aSegmentTooSmallForTheLargestRecordIsRefused() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new JournalWriter(
                        tempDir.resolve("tiny.journal"), SequencedEvent.MAX_MESSAGE_SIZE, segments::add));
    }

    @Test
    void sealingASegmentTwiceDoesNothing() throws IOException {
        JournalWriter writer = new JournalWriter(tempDir.resolve("twice.journal"), FILE_SIZE, segments::add);
        writer.append(0, 1, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
        writer.close();
        segments.get(0).seal();
        segments.get(0).seal();
        assertEquals(JournalWriter.HEADER_SIZE, Files.size(segments.get(0).path()));
    }
}
