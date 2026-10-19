package group.gnometrading.sequencer.metrics;

import group.gnometrading.schemas.MessageHeaderEncoder;
import group.gnometrading.schemas.MetricColumnEncoder;
import group.gnometrading.schemas.MetricRowEncoder;
import group.gnometrading.schemas.MetricStreamEncoder;
import group.gnometrading.sequencer.SequencedEvent;

/**
 * How large a metric row and a declared name may be. Both are what is left of a ring slot once the message around
 * them is counted, so metrics fit the slot the other messages set instead of growing it.
 */
public final class MetricLimits {

    /** The most bytes one row's values may take. */
    public static final int MAX_ROW_BYTES = SequencedEvent.MAX_MESSAGE_SIZE
            - MessageHeaderEncoder.ENCODED_LENGTH
            - MetricRowEncoder.BLOCK_LENGTH
            - MetricRowEncoder.valuesHeaderLength();

    /** The longest stream or column name, in ASCII characters. */
    public static final int MAX_NAME_BYTES = SequencedEvent.MAX_MESSAGE_SIZE
            - MessageHeaderEncoder.ENCODED_LENGTH
            - Math.max(MetricStreamEncoder.BLOCK_LENGTH, MetricColumnEncoder.BLOCK_LENGTH)
            - Math.max(MetricStreamEncoder.nameHeaderLength(), MetricColumnEncoder.nameHeaderLength());

    /** Stream ids are a uint16 in every row. */
    public static final int MAX_STREAMS = 1 << 16;

    private static final char FIRST_PRINTABLE = ' ';
    private static final char LAST_PRINTABLE = '~';

    private MetricLimits() {}

    static void checkName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A metric name can't be empty");
        }
        if (name.length() > MAX_NAME_BYTES) {
            throw new IllegalArgumentException(
                    "Metric name '" + name + "' is longer than " + MAX_NAME_BYTES + " characters");
        }
        for (int i = 0; i < name.length(); i++) {
            char character = name.charAt(i);
            if (character < FIRST_PRINTABLE || character > LAST_PRINTABLE) {
                throw new IllegalArgumentException("Metric name '" + name + "' must be printable ASCII");
            }
        }
    }
}
