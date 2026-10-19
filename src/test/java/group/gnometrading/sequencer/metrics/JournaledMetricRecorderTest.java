package group.gnometrading.sequencer.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.MetricColumnDecoder;
import group.gnometrading.schemas.MetricColumnType;
import group.gnometrading.schemas.MetricRow;
import group.gnometrading.schemas.MetricRowDecoder;
import group.gnometrading.schemas.MetricStreamDecoder;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.JournalReader;
import group.gnometrading.sequencer.JournalSegment;
import group.gnometrading.sequencer.JournalWriter;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournaledMetricRecorderTest {

    private static final int METRIC_SOURCE = 8;
    private static final int INTENT_SOURCE = 1;

    @TempDir
    Path tempDir;

    private final List<JournalSegment> segments = new ArrayList<>();
    private JournalWriter writer;
    private SequencedRingBuffer<MetricRow> metrics;
    private SequencedRingBuffer<Intent> intents;
    private SequencedPoller metricPoller;
    private SequencedPoller intentPoller;

    @BeforeEach
    void setUp() throws Exception {
        writer = new JournalWriter(tempDir.resolve("journal"), 1 << 20, segments::add);
        GlobalSequence sequence = new GlobalSequence();
        metrics = new SequencedRingBuffer<>(MetricRow::new, sequence);
        intents = new SequencedRingBuffer<>(Intent::new, sequence);
        metricPoller = metrics.createPoller(writer.source(METRIC_SOURCE));
        intentPoller = intents.createPoller(writer.source(INTENT_SOURCE));
    }

    @Test
    void declarationsThenRowsAreJournaledInOrderWithTheIntentsAroundThem() throws Exception {
        JournaledMetricRecorder recorder = new JournaledMetricRecorder(metrics);
        MetricStream stream = recorder.createStream("cs2_series_orders");
        int timestamp = stream.addLongColumn("timestamp");
        int fair = stream.addDoubleColumn("fair");
        int side = stream.addIntColumn("side");
        stream.freeze();

        stream.begin();
        stream.setLong(timestamp, 1_700_000_000_000L);
        stream.setDouble(fair, 0.62);
        stream.setInt(side, -1);
        stream.commit();
        intents.claim();
        intents.publish();
        stream.begin();
        stream.setLong(timestamp, 1_700_000_000_500L);
        stream.commit();
        drain();

        List<String> records = readAll();

        assertEquals(
                List.of(
                        "1 stream 0 cs2_series_orders columns=3",
                        "2 column 0/0 timestamp LONG",
                        "3 column 0/1 fair DOUBLE",
                        "4 column 0/2 side INT",
                        "5 row 0 1700000000000 0.62 -1",
                        "6 intent",
                        "7 row 0 1700000000500 0.0 0"),
                records);
    }

    @Test
    void eachStreamHasItsOwnIdAndRowWidth() throws Exception {
        JournaledMetricRecorder recorder = new JournaledMetricRecorder(metrics);
        MetricStream first = recorder.createStream("first");
        int a = first.addIntColumn("a");
        first.freeze();
        MetricStream second = recorder.createStream("second");
        int b = second.addDoubleColumn("b");
        second.freeze();

        second.begin();
        second.setDouble(b, 2.5);
        second.commit();
        first.begin();
        first.setInt(a, 9);
        first.commit();
        drain();

        List<String> rows =
                readAll().stream().filter(record -> record.contains(" row ")).toList();
        assertEquals(List.of("5 row 1 2.5", "6 row 0 9"), rows);
    }

    private void drain() throws Exception {
        while (metricPoller.poll() + intentPoller.poll() > 0) {
            continue;
        }
        writer.close();
    }

    /** Each record as text, decoding rows by the declarations read before them, as a journal reader must. */
    private List<String> readAll() throws Exception {
        List<String> records = new ArrayList<>();
        List<List<MetricColumnType>> columnTypes = new ArrayList<>();
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        for (JournalSegment segment : segments) {
            segment.seal();
            JournalReader reader = new JournalReader(segment.path());
            reader.readAll((sequence, sourceId, templateId, buffer, length) -> {
                if (templateId == IntentDecoder.TEMPLATE_ID) {
                    assertEquals(INTENT_SOURCE, sourceId);
                    records.add(sequence + " intent");
                    return;
                }
                assertEquals(METRIC_SOURCE, sourceId);
                if (templateId == MetricStreamDecoder.TEMPLATE_ID) {
                    MetricStreamDecoder stream = new MetricStreamDecoder().wrapAndApplyHeader(buffer, 0, header);
                    columnTypes.add(new ArrayList<>());
                    records.add(sequence + " stream " + stream.streamId() + " " + stream.name() + " columns="
                            + stream.columnCount());
                } else if (templateId == MetricColumnDecoder.TEMPLATE_ID) {
                    MetricColumnDecoder column = new MetricColumnDecoder().wrapAndApplyHeader(buffer, 0, header);
                    columnTypes.get(column.streamId()).add(column.columnType());
                    records.add(sequence + " column " + column.streamId() + "/" + column.columnIndex() + " "
                            + column.name() + " " + column.columnType());
                } else {
                    assertEquals(MetricRowDecoder.TEMPLATE_ID, templateId);
                    records.add(sequence + " row " + decodeRow(buffer, length, header, columnTypes));
                }
            });
        }
        return records;
    }

    private static String decodeRow(
            UnsafeBuffer buffer, int length, MessageHeaderDecoder header, List<List<MetricColumnType>> columnTypes) {
        MetricRowDecoder row = new MetricRowDecoder().wrapAndApplyHeader(buffer, 0, header);
        int streamId = row.streamId();
        int valuesLength = row.valuesLength();
        UnsafeBuffer values = new UnsafeBuffer(new byte[valuesLength]);
        row.getValues(values, 0, valuesLength);
        assertEquals(length, MessageHeaderDecoder.ENCODED_LENGTH + row.encodedLength());
        StringBuilder text = new StringBuilder().append(streamId);
        int offset = 0;
        for (MetricColumnType type : columnTypes.get(streamId)) {
            switch (type) {
                case LONG -> text.append(' ').append(values.getLong(offset, ByteOrder.LITTLE_ENDIAN));
                case DOUBLE -> text.append(' ').append(values.getDouble(offset, ByteOrder.LITTLE_ENDIAN));
                case INT -> text.append(' ').append(values.getInt(offset, ByteOrder.LITTLE_ENDIAN));
                default -> throw new IllegalStateException("Unknown column type " + type);
            }
            offset += MetricStream.width(type);
        }
        assertEquals(valuesLength, offset);
        return text.toString();
    }
}
