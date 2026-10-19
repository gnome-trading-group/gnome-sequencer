package group.gnometrading.sequencer.metrics;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ColumnarMetricRecorderTest {

    @Test
    void rowsLandInTheStreamsBufferByColumn() {
        ColumnarMetricRecorder recorder = new ColumnarMetricRecorder(2);
        MetricStream stream = recorder.createStream("signals");
        int timestamp = stream.addLongColumn("timestamp");
        int fair = stream.addDoubleColumn("fair");
        int level = stream.addIntColumn("level");
        stream.freeze();

        for (int i = 0; i < 3; i++) {
            stream.begin();
            stream.setLong(timestamp, 100L + i);
            stream.setDouble(fair, 0.5 + i);
            stream.setInt(level, i);
            stream.commit();
        }

        RecordBuffer buffer = recorder.getBuffers().get(0);
        assertEquals("signals", buffer.getName());
        assertEquals(3, buffer.getCount());
        assertEquals(
                List.of("timestamp", "fair", "level"),
                buffer.getColumns().stream().map(ColumnDef::name).toList());
        assertArrayEquals(new long[] {100, 101, 102}, Arrays.copyOf(buffer.getLongColumn(0), 3));
        assertArrayEquals(new double[] {0.5, 1.5, 2.5}, Arrays.copyOf(buffer.getDoubleColumn(0), 3));
        assertArrayEquals(new int[] {0, 1, 2}, Arrays.copyOf(buffer.getIntColumn(0), 3));
    }

    @Test
    void aColumnLeftUnsetIsZero() {
        ColumnarMetricRecorder recorder = new ColumnarMetricRecorder(4);
        MetricStream stream = recorder.createStream("signals");
        int fair = stream.addDoubleColumn("fair");
        stream.addDoubleColumn("edge");
        stream.freeze();

        stream.begin();
        stream.setDouble(fair, 0.4);
        stream.commit();

        assertEquals(0.0, recorder.getBuffers().get(0).getDoubleColumn(1)[0]);
    }

    @Test
    void onlyFrozenStreamsHaveABufferInTheOrderTheyFroze() {
        ColumnarMetricRecorder recorder = new ColumnarMetricRecorder(4);
        MetricStream first = recorder.createStream("first");
        MetricStream second = recorder.createStream("second");
        recorder.createStream("never_frozen").addLongColumn("timestamp");
        first.addLongColumn("timestamp");
        second.addLongColumn("timestamp");
        second.freeze();
        first.freeze();

        assertEquals(
                List.of("second", "first"),
                recorder.getBuffers().stream().map(RecordBuffer::getName).toList());
        assertThrows(IllegalArgumentException.class, () -> recorder.createStream("first"));
        assertTrue(recorder.getBuffers().stream().allMatch(buffer -> buffer.getCount() == 0));
    }
}
