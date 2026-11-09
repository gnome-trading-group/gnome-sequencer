package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.book.Mbp10Decoder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournalLostEventsTest {

    private static final long FILE_SIZE = 1024L * 1024L;
    private static final UnsafeBuffer EMPTY = new UnsafeBuffer(new byte[0]);
    private static final int WINDOW = 256;

    @TempDir
    Path tempDir;

    private final List<JournalSegment> segments = new ArrayList<>();

    private JournalWriter lossy(String name) throws IOException {
        return new JournalWriter(tempDir.resolve(name), FILE_SIZE, segments::add, true);
    }

    private static void append(JournalWriter writer, long globalSequence) {
        writer.append(1, globalSequence, Mbp10Decoder.TEMPLATE_ID, EMPTY, 0);
    }

    /** Each record as its sequence, each gap as "first-last". */
    private static final class Replay implements JournalRecordHandler {
        final List<String> seen = new ArrayList<>();

        @Override
        public void onRecord(long globalSequence, int sourceId, int templateId, UnsafeBuffer buffer, int length) {
            seen.add(String.valueOf(globalSequence));
        }

        @Override
        public void onGap(long firstLost, long lastLost) {
            seen.add(firstLost + "-" + lastLost);
        }
    }

    private List<String> replay() throws IOException {
        Replay replay = new Replay();
        long previous = 0;
        for (JournalSegment segment : segments) {
            JournalReader reader = new JournalReader(segment.path(), previous);
            reader.readAll(replay);
            previous = reader.lastReadSequence();
            assertEquals(segment.lastSequence(), previous);
        }
        return replay.seen;
    }

    @Test
    void skippingMissingWritesTheHoleAsOneGapAndReleasesWhatWaitedBehindIt() throws IOException {
        JournalWriter writer = lossy("skip.journal");
        append(writer, 1);
        append(writer, 2);
        append(writer, 5);
        append(writer, 6);
        assertEquals(2, writer.lastFlushedSequence());

        assertEquals(2, writer.skipMissing());
        assertEquals(6, writer.lastFlushedSequence());
        assertEquals(2, writer.lostSequences());
        writer.close();

        assertEquals(List.of("1", "2", "3-4", "5", "6"), replay());
    }

    @Test
    void separateHolesAreSeparateGaps() throws IOException {
        JournalWriter writer = lossy("holes.journal");
        append(writer, 2);
        append(writer, 4);
        append(writer, 7);
        writer.skipMissing();
        writer.close();

        assertEquals(List.of("1-1", "2", "3-3", "4", "5-6", "7"), replay());
    }

    @Test
    void anEventBeyondTheReorderWindowSkipsOnlyTheOldestMissing() throws IOException {
        JournalWriter writer = lossy("overflow.journal");
        append(writer, 1);
        for (long sequence = 3; sequence <= 1 + WINDOW; sequence++) {
            append(writer, sequence);
        }
        assertEquals(1, writer.lastFlushedSequence(), "A full window still waits for 2");

        append(writer, 2 + WINDOW);

        assertEquals(1, writer.lostSequences());
        assertEquals(2 + WINDOW, writer.lastFlushedSequence());
        writer.close();
        List<String> seen = replay();
        assertEquals(List.of("1", "2-2", "3"), seen.subList(0, 3));
        assertEquals(WINDOW + 2, seen.size());
    }

    @Test
    void aFarJumpIsWrittenAsGapsWithoutWalkingEverySequence() throws IOException {
        JournalWriter writer = lossy("jump.journal");
        append(writer, 1);
        append(writer, 1_000_000_000L);
        writer.close();

        assertEquals(1_000_000_000L - 2, writer.lostSequences());
        assertEquals(List.of("1", "2-999999744", "999999745-999999999", "1000000000"), replay());
    }

    @Test
    void anEventArrivingAfterItsGapIsDroppedNotWrittenTwice() throws IOException {
        JournalWriter writer = lossy("late.journal");
        append(writer, 1);
        append(writer, 3);
        writer.skipMissing();

        append(writer, 2);
        append(writer, 3);
        append(writer, 4);

        assertEquals(2, writer.lateEvents());
        writer.close();
        assertEquals(List.of("1", "2-2", "3", "4"), replay());
    }

    @Test
    void closingWritesWhatIsStillMissingSoNothingWaitingIsLeftOut() throws IOException {
        JournalWriter writer = lossy("close.journal");
        append(writer, 1);
        append(writer, 4);
        writer.close();

        assertEquals(List.of("1", "2-3", "4"), replay());
    }

    @Test
    void aGapStartingASegmentIsItsFirstSequenceAndReadingCarriesOn() throws IOException {
        JournalWriter writer = lossy("rolled.journal");
        append(writer, 1);
        writer.roll();
        append(writer, 5);
        writer.close();

        assertEquals(2, segments.size());
        assertEquals(2, segments.get(1).firstSequence());
        assertEquals(List.of("1", "2-4", "5"), replay());
    }

    @Test
    void aLappedReaderLeavesAGapForWhatItSkipped() throws Exception {
        JournalWriter writer = lossy("lapped.journal");
        GlobalSequence globalSequence = new GlobalSequence();
        SequencedRingBuffer<Intent> fast = new SequencedRingBuffer<>(Intent::new, globalSequence, 8);
        SequencedRingBuffer<Intent> slow = new SequencedRingBuffer<>(Intent::new, globalSequence);
        LappingPoller fastReader = fast.createLappingPoller(writer.source(1));
        SequencedPoller slowReader = slow.createPoller(writer.source(2));

        for (int i = 0; i < 20; i++) {
            fast.claim();
            fast.publish();
        }
        slow.claim();
        slow.publish();
        slowReader.poll();
        fastReader.poll();
        assertEquals(12, fastReader.lapped());
        assertEquals(0, writer.lastFlushedSequence(), "Everything waits behind what the lap lost");

        assertEquals(12, writer.skipMissing());
        assertEquals(21, writer.lastFlushedSequence());
        writer.close();

        List<String> seen = replay();
        assertEquals("1-12", seen.get(0));
        assertEquals(List.of("13", "14", "15", "16", "17", "18", "19", "20", "21"), seen.subList(1, seen.size()));
    }

    @Test
    void aReplayThatDoesNotHandleGapsFailsAtTheHole() throws IOException {
        JournalWriter writer = lossy("strict-replay.journal");
        append(writer, 2);
        writer.close();

        JournalReader reader = new JournalReader(segments.get(0).path());
        IllegalStateException hole =
                assertThrows(IllegalStateException.class, () -> reader.readAll((s, src, t, b, l) -> {}));
        assertTrue(hole.getMessage().contains("lost global sequences 1 to 1"), hole.getMessage());
    }

    @Test
    void aJournalThatNeedsEverySequenceCannotSkip() throws IOException {
        JournalWriter writer = new JournalWriter(tempDir.resolve("strict.journal"), FILE_SIZE, segments::add);
        append(writer, 2);

        assertThrows(IllegalStateException.class, writer::skipMissing);
        writer.close();
    }
}
