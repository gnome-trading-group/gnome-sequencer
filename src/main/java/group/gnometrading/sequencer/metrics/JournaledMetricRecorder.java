package group.gnometrading.sequencer.metrics;

import group.gnometrading.schemas.MessageHeaderEncoder;
import group.gnometrading.schemas.MetricColumnEncoder;
import group.gnometrading.schemas.MetricRowEncoder;
import group.gnometrading.schemas.MetricStreamEncoder;
import group.gnometrading.sequencer.SequencedEvent;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Publishes each stream's rows to a ring buffer on the session sequence, which the journal records alongside the
 * market data, intents and reports around them. Freezing a stream first publishes its declaration ({@code
 * MetricStream}, then a {@code MetricColumn} per column), so the journal names its own columns.
 *
 * <p>The ring has one producer: every stream of this recorder must be recorded by the same thread. Declarations are
 * published from whichever thread freezes them, which must be before that thread starts.
 */
public final class JournaledMetricRecorder implements MetricRecorder {

    private final SequencedRingBuffer<?> ringBuffer;
    private final Set<String> names = new HashSet<>();
    private int nextStreamId;

    public JournaledMetricRecorder(SequencedRingBuffer<?> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public MetricStream createStream(String name) {
        if (nextStreamId == MetricLimits.MAX_STREAMS) {
            throw new IllegalStateException("No more than " + MetricLimits.MAX_STREAMS + " metric streams");
        }
        if (!names.add(name)) {
            throw new IllegalArgumentException("A metric stream named " + name + " already exists");
        }
        return new Stream(name, nextStreamId++);
    }

    private final class Stream extends MetricStream {

        private final int streamId;
        private final UnsafeBuffer row = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
        private int[] offsets;
        private int valuesOffset;
        private int rowLength;

        Stream(String name, int streamId) {
            super(name);
            this.streamId = streamId;
        }

        @Override
        protected void onFreeze() {
            publishDeclarations();
            offsets = new int[columnCount()];
            int offset = 0;
            for (int column = 0; column < columnCount(); column++) {
                offsets[column] = offset;
                offset += width(columnType(column));
            }
            // The header, the stream id and the values' length are the same in every row, so they're written once.
            MessageHeaderEncoder header = new MessageHeaderEncoder();
            MetricRowEncoder encoder = new MetricRowEncoder()
                    .wrapAndApplyHeader(row, 0, header)
                    .streamId(streamId)
                    .putValues(new byte[rowBytes()], 0, rowBytes());
            rowLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
            valuesOffset = rowLength - rowBytes();
        }

        private void publishDeclarations() {
            UnsafeBuffer declaration = new UnsafeBuffer(new byte[SequencedEvent.MAX_MESSAGE_SIZE]);
            MessageHeaderEncoder header = new MessageHeaderEncoder();
            MetricStreamEncoder stream = new MetricStreamEncoder()
                    .wrapAndApplyHeader(declaration, 0, header)
                    .streamId(streamId)
                    .columnCount(columnCount())
                    .name(name());
            ringBuffer.publishRaw(
                    declaration,
                    MetricStreamEncoder.TEMPLATE_ID,
                    MessageHeaderEncoder.ENCODED_LENGTH + stream.encodedLength());
            MetricColumnEncoder columnEncoder = new MetricColumnEncoder();
            for (int column = 0; column < columnCount(); column++) {
                columnEncoder
                        .wrapAndApplyHeader(declaration, 0, header)
                        .streamId(streamId)
                        .columnIndex(column)
                        .columnType(columnType(column))
                        .name(columnName(column));
                ringBuffer.publishRaw(
                        declaration,
                        MetricColumnEncoder.TEMPLATE_ID,
                        MessageHeaderEncoder.ENCODED_LENGTH + columnEncoder.encodedLength());
            }
        }

        @Override
        protected void onBegin() {
            row.setMemory(valuesOffset, rowBytes(), (byte) 0);
        }

        @Override
        protected void writeLong(int column, long value) {
            row.putLong(valuesOffset + offsets[column], value, ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        protected void writeDouble(int column, double value) {
            row.putDouble(valuesOffset + offsets[column], value, ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        protected void writeInt(int column, int value) {
            row.putInt(valuesOffset + offsets[column], value, ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        protected void onCommit() {
            ringBuffer.publishRaw(row, MetricRowEncoder.TEMPLATE_ID, rowLength);
        }
    }
}
