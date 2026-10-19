package group.gnometrading.sequencer.metrics;

import group.gnometrading.schemas.MetricColumnType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Records each stream into a {@link RecordBuffer} in memory, one array per column, for a backtest to read back
 * once it ends.
 */
public final class ColumnarMetricRecorder implements MetricRecorder {

    public static final int DEFAULT_INITIAL_CAPACITY = 1_000_000;

    private final int initialCapacity;
    private final Set<String> names = new HashSet<>();
    private final List<RecordBuffer> buffers = new ArrayList<>();

    public ColumnarMetricRecorder() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    /** @param initialCapacity each stream's starting row capacity; it doubles whenever it fills */
    public ColumnarMetricRecorder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    @Override
    public MetricStream createStream(String name) {
        if (!names.add(name)) {
            throw new IllegalArgumentException("A metric stream named " + name + " already exists");
        }
        return new Stream(name);
    }

    /** Every frozen stream's buffer, in the order they were frozen. */
    public List<RecordBuffer> getBuffers() {
        return Collections.unmodifiableList(buffers);
    }

    private final class Stream extends MetricStream {

        private RecordBuffer buffer;
        // A RecordBuffer numbers its columns per type; this maps the stream's column index onto those.
        private int[] typeIndexes;
        private int row;

        Stream(String name) {
            super(name);
        }

        @Override
        protected void onFreeze() {
            buffer = new RecordBuffer(name(), initialCapacity);
            typeIndexes = new int[columnCount()];
            for (int column = 0; column < columnCount(); column++) {
                typeIndexes[column] = addColumn(columnType(column), columnName(column));
            }
            buffer.freeze();
            buffers.add(buffer);
        }

        private int addColumn(MetricColumnType type, String columnName) {
            return switch (type) {
                case LONG -> buffer.addLongColumn(columnName);
                case DOUBLE -> buffer.addDoubleColumn(columnName);
                case INT -> buffer.addIntColumn(columnName);
                default -> throw new IllegalArgumentException("Unknown metric column type " + type);
            };
        }

        @Override
        protected void onBegin() {
            row = buffer.appendRow();
        }

        @Override
        protected void writeLong(int column, long value) {
            buffer.setLong(row, typeIndexes[column], value);
        }

        @Override
        protected void writeDouble(int column, double value) {
            buffer.setDouble(row, typeIndexes[column], value);
        }

        @Override
        protected void writeInt(int column, int value) {
            buffer.setInt(row, typeIndexes[column], value);
        }

        @Override
        protected void onCommit() {}
    }
}
