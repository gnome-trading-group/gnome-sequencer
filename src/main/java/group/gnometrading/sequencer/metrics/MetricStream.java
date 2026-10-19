package group.gnometrading.sequencer.metrics;

import group.gnometrading.schemas.MetricColumnType;
import java.util.ArrayList;
import java.util.List;

/**
 * One named stream of metric rows, with columns declared at runtime.
 *
 * <p>Declared first: {@code addLongColumn}, {@code addDoubleColumn} and {@code addIntColumn} each return the new
 * column's index, and {@link #freeze()} fixes the columns. Recorded after, on the hot path and without allocating:
 * {@link #begin()}, a {@code set} per column, then {@link #commit()}. A column left unset in a row is 0.
 *
 * <p>Where rows go depends on the recorder: columns in memory for a backtest, the session journal for a live or
 * paper session, nowhere when nothing records them. Not thread-safe: one thread records a stream.
 */
public abstract class MetricStream {

    private final String name;
    private final List<String> columnNames = new ArrayList<>();
    private final List<MetricColumnType> declaredTypes = new ArrayList<>();
    private MetricColumnType[] columnTypes;
    private int rowBytes;
    private boolean frozen;
    private boolean rowOpen;

    protected MetricStream(String name) {
        MetricLimits.checkName(name);
        this.name = name;
    }

    public final String name() {
        return name;
    }

    public final int addLongColumn(String columnName) {
        return addColumn(columnName, MetricColumnType.LONG);
    }

    public final int addDoubleColumn(String columnName) {
        return addColumn(columnName, MetricColumnType.DOUBLE);
    }

    public final int addIntColumn(String columnName) {
        return addColumn(columnName, MetricColumnType.INT);
    }

    private int addColumn(String columnName, MetricColumnType type) {
        if (frozen) {
            throw new IllegalStateException("Metric stream '" + name + "' is frozen; columns can't be added");
        }
        MetricLimits.checkName(columnName);
        if (columnNames.contains(columnName)) {
            throw new IllegalArgumentException("Metric stream '" + name + "' already has a column " + columnName);
        }
        if (rowBytes + width(type) > MetricLimits.MAX_ROW_BYTES) {
            throw new IllegalArgumentException("Metric stream '" + name + "' can't fit column " + columnName
                    + ": a row holds at most " + MetricLimits.MAX_ROW_BYTES + " bytes");
        }
        rowBytes += width(type);
        columnNames.add(columnName);
        declaredTypes.add(type);
        return columnNames.size() - 1;
    }

    /** Fixes the columns; rows can be recorded from now on. */
    public final void freeze() {
        if (frozen) {
            throw new IllegalStateException("Metric stream '" + name + "' is already frozen");
        }
        if (columnNames.isEmpty()) {
            throw new IllegalStateException("Metric stream '" + name + "' has no columns");
        }
        columnTypes = declaredTypes.toArray(new MetricColumnType[0]);
        frozen = true;
        onFreeze();
    }

    public final boolean isFrozen() {
        return frozen;
    }

    public final int columnCount() {
        return columnNames.size();
    }

    public final String columnName(int column) {
        return columnNames.get(column);
    }

    public final MetricColumnType columnType(int column) {
        return declaredTypes.get(column);
    }

    /** The bytes one row's values take: each column's width, in declaration order. */
    public final int rowBytes() {
        return rowBytes;
    }

    /** Starts a row. */
    public final void begin() {
        if (!frozen) {
            throw new IllegalStateException("Metric stream '" + name + "' must be frozen before recording");
        }
        if (rowOpen) {
            throw new IllegalStateException("Metric stream '" + name + "' already has a row open");
        }
        rowOpen = true;
        onBegin();
    }

    public final void setLong(int column, long value) {
        checkColumn(column, MetricColumnType.LONG);
        writeLong(column, value);
    }

    public final void setDouble(int column, double value) {
        checkColumn(column, MetricColumnType.DOUBLE);
        writeDouble(column, value);
    }

    public final void setInt(int column, int value) {
        checkColumn(column, MetricColumnType.INT);
        writeInt(column, value);
    }

    /** Ends the row and records it. */
    public final void commit() {
        if (!rowOpen) {
            throw new IllegalStateException("Metric stream '" + name + "' has no row open");
        }
        rowOpen = false;
        onCommit();
    }

    private void checkColumn(int column, MetricColumnType type) {
        if (!rowOpen) {
            throw new IllegalStateException("Metric stream '" + name + "' has no row open");
        }
        if (column < 0 || column >= columnTypes.length || columnTypes[column] != type) {
            throw new IllegalArgumentException(
                    "Metric stream '" + name + "' has no " + type + " column at index " + column);
        }
    }

    static int width(MetricColumnType type) {
        return switch (type) {
            case LONG, DOUBLE -> Long.BYTES;
            case INT -> Integer.BYTES;
            default -> throw new IllegalArgumentException("No width for metric column type " + type);
        };
    }

    protected abstract void onFreeze();

    protected abstract void onBegin();

    protected abstract void writeLong(int column, long value);

    protected abstract void writeDouble(int column, double value);

    protected abstract void writeInt(int column, int value);

    protected abstract void onCommit();
}
