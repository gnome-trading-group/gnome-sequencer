package group.gnometrading.sequencer.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.MetricColumnType;
import org.junit.jupiter.api.Test;

class MetricStreamTest {

    @Test
    void columnsAreNumberedInDeclarationOrderAcrossTypes() {
        MetricStream stream = MetricRecorder.discarding().createStream("signals");

        assertEquals(0, stream.addLongColumn("timestamp"));
        assertEquals(1, stream.addDoubleColumn("fair"));
        assertEquals(2, stream.addIntColumn("level"));
        assertEquals(3, stream.columnCount());
        assertEquals(MetricColumnType.DOUBLE, stream.columnType(1));
        assertEquals("level", stream.columnName(2));
        assertEquals(Long.BYTES + Double.BYTES + Integer.BYTES, stream.rowBytes());
    }

    @Test
    void aRowNeedsAFrozenStreamAndOneRowAtATime() {
        MetricStream stream = MetricRecorder.discarding().createStream("signals");
        int column = stream.addDoubleColumn("fair");

        assertThrows(IllegalStateException.class, stream::begin);
        stream.freeze();
        assertThrows(IllegalStateException.class, () -> stream.setDouble(column, 1.0));
        assertThrows(IllegalStateException.class, stream::commit);
        stream.begin();
        assertThrows(IllegalStateException.class, stream::begin);
        stream.setDouble(column, 1.0);
        stream.commit();
    }

    @Test
    void settingAColumnAsTheWrongTypeOrOutOfRangeIsRefused() {
        MetricStream stream = MetricRecorder.discarding().createStream("signals");
        int fair = stream.addDoubleColumn("fair");
        stream.freeze();
        stream.begin();

        assertThrows(IllegalArgumentException.class, () -> stream.setLong(fair, 1L));
        assertThrows(IllegalArgumentException.class, () -> stream.setDouble(fair + 1, 1.0));
        assertThrows(IllegalArgumentException.class, () -> stream.setDouble(-1, 1.0));
    }

    @Test
    void aFrozenStreamTakesNoMoreColumnsAndFreezesOnce() {
        MetricStream stream = MetricRecorder.discarding().createStream("signals");
        stream.addLongColumn("timestamp");
        stream.freeze();

        assertTrue(stream.isFrozen());
        assertThrows(IllegalStateException.class, () -> stream.addLongColumn("late"));
        assertThrows(IllegalStateException.class, stream::freeze);
    }

    @Test
    void aStreamWithoutColumnsCannotFreeze() {
        MetricStream stream = MetricRecorder.discarding().createStream("signals");

        assertThrows(IllegalStateException.class, stream::freeze);
    }

    @Test
    void aRowWiderThanASlotIsRefusedWhenTheColumnIsDeclared() {
        MetricStream stream = MetricRecorder.discarding().createStream("wide");
        int columns = MetricLimits.MAX_ROW_BYTES / Long.BYTES;
        for (int i = 0; i < columns; i++) {
            stream.addLongColumn("c" + i);
        }

        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> stream.addLongColumn("one_too_many"));
        assertTrue(error.getMessage().contains(String.valueOf(MetricLimits.MAX_ROW_BYTES)), error.getMessage());
        assertEquals(columns, stream.columnCount());
    }

    @Test
    void namesMustBeShortPrintableAsciiAndUnique() {
        MetricRecorder recorder = MetricRecorder.discarding();
        MetricStream stream = recorder.createStream("signals");

        assertThrows(IllegalArgumentException.class, () -> recorder.createStream("signals"));
        assertThrows(IllegalArgumentException.class, () -> recorder.createStream(""));
        assertThrows(IllegalArgumentException.class, () -> recorder.createStream("fairé"));
        assertThrows(
                IllegalArgumentException.class,
                () -> recorder.createStream("x".repeat(MetricLimits.MAX_NAME_BYTES + 1)));
        stream.addLongColumn("timestamp");
        assertThrows(IllegalArgumentException.class, () -> stream.addDoubleColumn("timestamp"));
        assertThrows(IllegalArgumentException.class, () -> stream.addDoubleColumn("tab\there"));
        recorder.createStream("x".repeat(MetricLimits.MAX_NAME_BYTES));
    }
}
