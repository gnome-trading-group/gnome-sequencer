package group.gnometrading.sequencer.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Named, typed, columnar append-only buffer.
 *
 * <p>Use is split into two phases:
 * <ol>
 *   <li><b>Schema definition</b> — call {@code addLongColumn}, {@code addDoubleColumn}, etc. to
 *       register columns, then call {@link #freeze()} to allocate backing arrays and lock the schema.
 *   <li><b>Recording</b> — call {@link #appendRow()} to get a row index, then write values with
 *       {@code setLong}, {@code setDouble}, etc. Rows grow by 2× via {@link Arrays#copyOf} when full.
 * </ol>
 *
 * <p>Each type maintains its own index space: the first {@code addLongColumn} returns 0, the second
 * returns 1, independently of {@code addDoubleColumn} indices.
 *
 * <p>All write methods are bounds-checked only up to the registered column count; they do not
 * validate the row index for speed.
 */
public final class RecordBuffer {

    private final String name;
    private final List<ColumnDef> columns = new ArrayList<>();
    private boolean frozen = false;

    private int longColCount = 0;
    private int doubleColCount = 0;
    private int intColCount = 0;
    private int byteColCount = 0;
    private int stringColCount = 0;

    private long[][] longColumns;
    private double[][] doubleColumns;
    private int[][] intColumns;
    private byte[][] byteColumns;
    private String[][] stringColumns;

    private int count = 0;
    private int capacity;

    public RecordBuffer(String name, int initialCapacity) {
        this.name = name;
        // At least one row, or doubling an empty buffer never makes room.
        this.capacity = Math.max(1, initialCapacity);
    }

    // =========================================================================
    // Schema definition phase
    // =========================================================================

    public int addLongColumn(String colName) {
        checkUnfrozen();
        int idx = longColCount++;
        columns.add(new ColumnDef(colName, ColumnDef.ColumnType.LONG, idx));
        return idx;
    }

    public int addDoubleColumn(String colName) {
        checkUnfrozen();
        int idx = doubleColCount++;
        columns.add(new ColumnDef(colName, ColumnDef.ColumnType.DOUBLE, idx));
        return idx;
    }

    public int addIntColumn(String colName) {
        checkUnfrozen();
        int idx = intColCount++;
        columns.add(new ColumnDef(colName, ColumnDef.ColumnType.INT, idx));
        return idx;
    }

    public int addByteColumn(String colName) {
        checkUnfrozen();
        int idx = byteColCount++;
        columns.add(new ColumnDef(colName, ColumnDef.ColumnType.BYTE, idx));
        return idx;
    }

    public int addStringColumn(String colName) {
        checkUnfrozen();
        int idx = stringColCount++;
        columns.add(new ColumnDef(colName, ColumnDef.ColumnType.STRING, idx));
        return idx;
    }

    /**
     * Locks the schema and allocates backing arrays.
     * Must be called before any recording.
     */
    public void freeze() {
        if (frozen) {
            throw new IllegalStateException("RecordBuffer '" + name + "' is already frozen");
        }
        longColumns = longColCount > 0 ? new long[longColCount][capacity] : new long[0][];
        doubleColumns = doubleColCount > 0 ? new double[doubleColCount][capacity] : new double[0][];
        intColumns = intColCount > 0 ? new int[intColCount][capacity] : new int[0][];
        byteColumns = byteColCount > 0 ? new byte[byteColCount][capacity] : new byte[0][];
        stringColumns = stringColCount > 0 ? new String[stringColCount][capacity] : new String[0][];
        frozen = true;
    }

    // =========================================================================
    // Recording phase
    // =========================================================================

    /**
     * Appends a new row and returns its index. Grows arrays if needed.
     */
    public int appendRow() {
        checkFrozen();
        if (count == capacity) {
            grow();
        }
        return count++;
    }

    public void setLong(int row, int col, long value) {
        longColumns[col][row] = value;
    }

    public void setDouble(int row, int col, double value) {
        doubleColumns[col][row] = value;
    }

    public void setInt(int row, int col, int value) {
        intColumns[col][row] = value;
    }

    public void setByte(int row, int col, byte value) {
        byteColumns[col][row] = value;
    }

    public void setString(int row, int col, String value) {
        stringColumns[col][row] = value;
    }

    // =========================================================================
    // Getters for Python bridge
    // =========================================================================

    public String getName() {
        return name;
    }

    public int getCount() {
        return count;
    }

    public List<ColumnDef> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public long[] getLongColumn(int idx) {
        return longColumns[idx];
    }

    public double[] getDoubleColumn(int idx) {
        return doubleColumns[idx];
    }

    public int[] getIntColumn(int idx) {
        return intColumns[idx];
    }

    public byte[] getByteColumn(int idx) {
        return byteColumns[idx];
    }

    public String[] getStringColumn(int idx) {
        return stringColumns[idx];
    }

    // =========================================================================
    // Housekeeping
    // =========================================================================

    public void clear() {
        count = 0;
    }

    private void grow() {
        int nc = capacity * 2;
        for (int ii = 0; ii < longColCount; ii++) {
            longColumns[ii] = Arrays.copyOf(longColumns[ii], nc);
        }
        for (int ii = 0; ii < doubleColCount; ii++) {
            doubleColumns[ii] = Arrays.copyOf(doubleColumns[ii], nc);
        }
        for (int ii = 0; ii < intColCount; ii++) {
            intColumns[ii] = Arrays.copyOf(intColumns[ii], nc);
        }
        for (int ii = 0; ii < byteColCount; ii++) {
            byteColumns[ii] = Arrays.copyOf(byteColumns[ii], nc);
        }
        for (int ii = 0; ii < stringColCount; ii++) {
            stringColumns[ii] = Arrays.copyOf(stringColumns[ii], nc);
        }
        capacity = nc;
    }

    private void checkUnfrozen() {
        if (frozen) {
            throw new IllegalStateException("Cannot add columns to frozen RecordBuffer '" + name + "'");
        }
    }

    private void checkFrozen() {
        if (!frozen) {
            throw new IllegalStateException("RecordBuffer '" + name + "' must be frozen before recording");
        }
    }
}
