package group.gnometrading.sequencer.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class RecordBufferTest {

    @Test
    void testAddColumnsAndFreeze() {
        RecordBuffer buf = new RecordBuffer("test", 8);
        int longIdx = buf.addLongColumn("ts");
        int doubleIdx = buf.addDoubleColumn("value");
        int intIdx = buf.addIntColumn("count");
        int byteIdx = buf.addByteColumn("flags");
        int stringIdx = buf.addStringColumn("label");
        buf.freeze();

        List<ColumnDef> cols = buf.getColumns();
        assertEquals(5, cols.size());
        assertEquals("ts", cols.get(0).name());
        assertEquals(ColumnDef.ColumnType.LONG, cols.get(0).type());
        assertEquals(0, longIdx);
        assertEquals(0, doubleIdx);
        assertEquals(0, intIdx);
        assertEquals(0, byteIdx);
        assertEquals(0, stringIdx);
    }

    @Test
    void testPerTypeIndexSpacesAreIndependent() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        int long0 = buf.addLongColumn("a");
        int double0 = buf.addDoubleColumn("b");
        int long1 = buf.addLongColumn("c");
        int double1 = buf.addDoubleColumn("d");
        buf.freeze();

        assertEquals(0, long0);
        assertEquals(0, double0);
        assertEquals(1, long1);
        assertEquals(1, double1);
    }

    @Test
    void testAppendRowAndReadBack() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        int tsCol = buf.addLongColumn("ts");
        int valCol = buf.addDoubleColumn("val");
        int cntCol = buf.addIntColumn("cnt");
        int flagCol = buf.addByteColumn("flag");
        int lblCol = buf.addStringColumn("label");
        buf.freeze();

        int row = buf.appendRow();
        buf.setLong(row, tsCol, 1_000_000L);
        buf.setDouble(row, valCol, 3.14);
        buf.setInt(row, cntCol, 42);
        buf.setByte(row, flagCol, (byte) 7);
        buf.setString(row, lblCol, "hello");

        assertEquals(1, buf.getCount());
        assertEquals(1_000_000L, buf.getLongColumn(tsCol)[row]);
        assertEquals(3.14, buf.getDoubleColumn(valCol)[row], 1e-10);
        assertEquals(42, buf.getIntColumn(cntCol)[row]);
        assertEquals((byte) 7, buf.getByteColumn(flagCol)[row]);
        assertEquals("hello", buf.getStringColumn(lblCol)[row]);
    }

    @Test
    void testMultipleRows() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        int col = buf.addLongColumn("x");
        buf.freeze();

        for (int ii = 0; ii < 3; ii++) {
            int row = buf.appendRow();
            buf.setLong(row, col, ii * 10L);
        }

        assertEquals(3, buf.getCount());
        long[] data = buf.getLongColumn(col);
        assertEquals(0L, data[0]);
        assertEquals(10L, data[1]);
        assertEquals(20L, data[2]);
    }

    @Test
    void testGrowBeyondInitialCapacity() {
        RecordBuffer buf = new RecordBuffer("test", 2);
        int col = buf.addLongColumn("v");
        buf.freeze();

        // Write more rows than initial capacity to trigger grow
        for (int ii = 0; ii < 5; ii++) {
            int row = buf.appendRow();
            buf.setLong(row, col, ii);
        }

        assertEquals(5, buf.getCount());
        long[] data = buf.getLongColumn(col);
        for (int ii = 0; ii < 5; ii++) {
            assertEquals(ii, data[ii]);
        }
    }

    @Test
    void testClearResetsCount() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        int col = buf.addLongColumn("x");
        buf.freeze();

        buf.appendRow();
        buf.appendRow();
        assertEquals(2, buf.getCount());

        buf.clear();
        assertEquals(0, buf.getCount());
    }

    @Test
    void testFreezeTwiceThrows() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        buf.addLongColumn("x");
        buf.freeze();

        assertThrows(IllegalStateException.class, buf::freeze);
    }

    @Test
    void testAddColumnAfterFreezeThrows() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        buf.freeze();

        assertThrows(IllegalStateException.class, () -> buf.addLongColumn("x"));
        assertThrows(IllegalStateException.class, () -> buf.addDoubleColumn("y"));
        assertThrows(IllegalStateException.class, () -> buf.addIntColumn("z"));
    }

    @Test
    void testAppendRowBeforeFreezeThrows() {
        RecordBuffer buf = new RecordBuffer("test", 4);
        buf.addLongColumn("x");

        assertThrows(IllegalStateException.class, buf::appendRow);
    }

    @Test
    void testGetNameReturnsConstructorName() {
        RecordBuffer buf = new RecordBuffer("my_stream", 4);
        buf.freeze();
        assertEquals("my_stream", buf.getName());
    }

    @Test
    void testBufferWithNoColumnsCanBeFrozenAndUsed() {
        RecordBuffer buf = new RecordBuffer("empty", 4);
        buf.freeze();
        int row = buf.appendRow();
        assertEquals(0, row);
        assertEquals(1, buf.getCount());
    }

    @Test
    void zeroInitialCapacityStillGrows() {
        RecordBuffer buffer = new RecordBuffer("empty", 0);
        int col = buffer.addLongColumn("value");
        buffer.freeze();
        for (int i = 0; i < 5; i++) {
            buffer.setLong(buffer.appendRow(), col, i);
        }
        assertEquals(5, buffer.getCount());
        assertEquals(4L, buffer.getLongColumn(col)[4]);
    }
}
