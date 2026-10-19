package group.gnometrading.sequencer.metrics;

/**
 * Describes a single column in a {@link RecordBuffer}.
 *
 * <p>{@code columnIndex} is the index into the type-specific array inside the buffer,
 * not the logical column ordinal. Two columns of different types can share the same
 * {@code columnIndex}.
 */
public record ColumnDef(String name, ColumnDef.ColumnType type, int columnIndex) {

    public enum ColumnType {
        LONG,
        DOUBLE,
        INT,
        BYTE,
        STRING
    }
}
