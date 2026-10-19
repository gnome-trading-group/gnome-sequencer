package group.gnometrading.sequencer.metrics;

import java.util.HashSet;
import java.util.Set;

/** Streams declared and recorded the same way as anywhere else, so a mistake fails here too; rows go nowhere. */
final class DiscardingMetricRecorder implements MetricRecorder {

    private final Set<String> names = new HashSet<>();

    @Override
    public MetricStream createStream(String name) {
        if (!names.add(name)) {
            throw new IllegalArgumentException("A metric stream named " + name + " already exists");
        }
        return new Stream(name);
    }

    private static final class Stream extends MetricStream {

        Stream(String name) {
            super(name);
        }

        @Override
        protected void onFreeze() {}

        @Override
        protected void onBegin() {}

        @Override
        protected void writeLong(int column, long value) {}

        @Override
        protected void writeDouble(int column, double value) {}

        @Override
        protected void writeInt(int column, int value) {}

        @Override
        protected void onCommit() {}
    }
}
