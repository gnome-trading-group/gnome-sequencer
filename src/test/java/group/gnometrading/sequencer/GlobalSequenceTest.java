package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import java.util.List;
import org.junit.jupiter.api.Test;

class GlobalSequenceTest {

    @Test
    void startsAtZeroAndIncrements() {
        GlobalSequence seq = new GlobalSequence();
        assertEquals(0, seq.current());
        assertEquals(1, seq.next());
        assertEquals(2, seq.next());
        assertEquals(3, seq.next());
        assertEquals(3, seq.current());
    }

    @Test
    void isMonotonicallyIncreasingUnderConcurrency() throws InterruptedException {
        GlobalSequence seq = new GlobalSequence();
        int threadCount = 4;
        int incrementsPerThread = 10_000;
        long[] results = new long[threadCount * incrementsPerThread];
        Thread[] threads = new Thread[threadCount];

        for (int threadIdx = 0; threadIdx < threadCount; threadIdx++) {
            final int offset = threadIdx * incrementsPerThread;
            threads[threadIdx] = new Thread(() -> {
                for (int i = 0; i < incrementsPerThread; i++) {
                    results[offset + i] = seq.next();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        // All results must be unique and within expected range
        boolean[] seen = new boolean[threadCount * incrementsPerThread + 1];
        for (long result : results) {
            assertTrue(result >= 1 && result <= threadCount * incrementsPerThread);
            seen[(int) result] = true;
        }
        for (int i = 1; i <= threadCount * incrementsPerThread; i++) {
            assertTrue(seen[i], "Missing sequence number: " + i);
        }
    }

    @Test
    void registersEveryBufferConstructedOnIt() {
        GlobalSequence shared = new GlobalSequence();
        GlobalSequence other = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> first = new SequencedRingBuffer<>(Mbp10Schema::new, shared);
        SequencedRingBuffer<Intent> second = new SequencedRingBuffer<>(Intent::new, shared);
        SequencedRingBuffer<Intent> elsewhere = new SequencedRingBuffer<>(Intent::new, other);

        assertEquals(List.of(first, second), shared.buffers());
        assertEquals(List.of(elsewhere), other.buffers());
        assertThrows(UnsupportedOperationException.class, () -> shared.buffers().clear());
    }
}
