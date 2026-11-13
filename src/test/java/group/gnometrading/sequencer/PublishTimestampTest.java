package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.book.Mbp10Decoder;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PublishTimestampTest {

    private static final long FILE_SIZE = 1024L * 1024L;
    private static final UnsafeBuffer PAYLOAD = new UnsafeBuffer(new byte[16]);

    @TempDir
    Path tempDir;

    private long now = 1_700_000_000_000_000_000L;

    private long tick() {
        return now += 1_000;
    }

    @Test
    void aPollerHandsOnWhenEachEventWasPublished() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), 64, this::tick);
        final List<Long> seen = new ArrayList<>();
        final SequencedPoller[] poller = new SequencedPoller[1];
        poller[0] = ring.createPoller((sequence, templateId, buffer, length) -> seen.add(poller[0].timestampNanos()));

        ring.claim();
        ring.publish();
        ring.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        ring.tryPublishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        poller[0].poll();

        final long first = 1_700_000_000_000_001_000L;
        assertEquals(List.of(first, first + 1_000, first + 2_000), seen);
    }

    @Test
    void aLappingPollerHandsOnWhenEachEventWasPublished() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence(), 64, this::tick);
        final List<Long> seen = new ArrayList<>();
        final LappingPoller[] poller = new LappingPoller[1];
        poller[0] = ring.createLappingPoller(
                (sequence, templateId, buffer, length) -> seen.add(poller[0].timestampNanos()));

        ring.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        ring.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        poller[0].poll();

        assertEquals(List.of(1_700_000_000_000_001_000L, 1_700_000_000_000_002_000L), seen);
    }

    @Test
    void theJournalKeepsWhenEachEventWasPublished() throws Exception {
        final GlobalSequence sequence = new GlobalSequence();
        final SequencedRingBuffer<Mbp10Schema> first =
                new SequencedRingBuffer<>(Mbp10Schema::new, sequence, 64, this::tick);
        final SequencedRingBuffer<Mbp10Schema> second =
                new SequencedRingBuffer<>(Mbp10Schema::new, sequence, 64, this::tick);
        final List<JournalSegment> segments = new ArrayList<>();
        final JournalWriter writer = new JournalWriter(tempDir, FILE_SIZE, segments::add);
        final SequencedPoller firstReader = writer.journal(first, 1);
        final SequencedPoller secondReader = writer.journal(second, 2);

        first.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        second.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        first.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
        secondReader.poll();
        firstReader.poll();
        writer.close();

        final List<String> records = new ArrayList<>();
        new JournalReader(segments.get(0).path())
                .readAll((globalSequence, timestampNanos, sourceId, templateId, buffer, length) ->
                        records.add(globalSequence + "@" + timestampNanos + " from " + sourceId));
        assertEquals(
                List.of("1@1700000000000001000 from 1", "2@1700000000000002000 from 2", "3@1700000000000003000 from 1"),
                records);
    }

    @Test
    void aSegmentFromBeforeTimestampsReadsWithoutThem() throws IOException {
        final ByteBuffer legacy = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        legacy.putLong(1)
                .putShort((short) 7)
                .putShort((short) Mbp10Decoder.TEMPLATE_ID)
                .putShort((short) 2);
        legacy.put((byte) 1).put((byte) 2);
        legacy.putLong(2)
                .putShort((short) 7)
                .putShort((short) Mbp10Decoder.TEMPLATE_ID)
                .putShort((short) 0);
        final Path segment = tempDir.resolve(JournalWriter.sealedFileName(0));
        Files.write(segment, legacy.array());

        final List<String> records = new ArrayList<>();
        new JournalReader(segment)
                .readAll((globalSequence, timestampNanos, sourceId, templateId, buffer, length) ->
                        records.add(globalSequence + "@" + timestampNanos + " " + length + " bytes"));

        assertEquals(List.of("1@" + JournalReader.NO_TIMESTAMP + " 2 bytes", "2@0 0 bytes"), records);
    }

    @Test
    void aSegmentInANewerFormatIsRefused() throws IOException {
        final ByteBuffer newer = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        newer.putInt(JournalWriter.SEGMENT_MAGIC).putShort((short) (JournalWriter.FORMAT + 1));
        final Path segment = tempDir.resolve(JournalWriter.sealedFileName(0));
        Files.write(segment, newer.array());

        final IllegalStateException refused =
                assertThrows(IllegalStateException.class, () -> new JournalReader(segment));
        assertTrue(refused.getMessage().contains("format " + (JournalWriter.FORMAT + 1)), refused.getMessage());
    }

    @Test
    void publishingRawAndPollingAllocateNothing() throws Exception {
        final SequencedRingBuffer<Mbp10Schema> ring = new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        final long[] sink = new long[1];
        final SequencedPoller[] poller = new SequencedPoller[1];
        poller[0] = ring.createPoller((sequence, templateId, buffer, length) -> sink[0] += poller[0].timestampNanos());
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().getId();

        cycle(ring, poller[0], 50_000);
        final long before = threads.getThreadAllocatedBytes(threadId);
        cycle(ring, poller[0], 10_000);
        final long allocated = threads.getThreadAllocatedBytes(threadId) - before;

        assertTrue(sink[0] != 0);
        // Measuring can itself allocate a little; anything per event would be hundreds of kilobytes here.
        assertTrue(allocated < 2_048, "allocated " + allocated + " bytes over 10000 events");
    }

    private static void cycle(final SequencedRingBuffer<Mbp10Schema> ring, final SequencedPoller poller, final int n)
            throws Exception {
        for (int i = 0; i < n; i++) {
            ring.publishRaw(PAYLOAD, 0, Mbp10Decoder.TEMPLATE_ID, PAYLOAD.capacity());
            poller.poll();
        }
    }
}
