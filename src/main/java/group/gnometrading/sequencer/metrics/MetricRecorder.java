package group.gnometrading.sequencer.metrics;

/**
 * Where a component declares its metric streams. The same calls record into memory for a backtest, into the session
 * journal for a live or paper session, or nowhere, so code that records metrics never knows which.
 */
public interface MetricRecorder {

    /**
     * Declares a new stream. Allocates; call while wiring, never on the hot path.
     *
     * @param name unique within this recorder; the stream's name wherever its rows end up
     * @throws IllegalArgumentException if the name is taken or isn't a valid metric name
     */
    MetricStream createStream(String name);

    /** A recorder whose streams accept rows and keep none, for runs that record nothing. */
    static MetricRecorder discarding() {
        return new DiscardingMetricRecorder();
    }
}
