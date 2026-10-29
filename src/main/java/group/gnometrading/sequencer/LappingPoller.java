package group.gnometrading.sequencer;

import com.lmax.disruptor.RingBuffer;
import group.gnometrading.utils.ByteBufferUtils;
import java.lang.invoke.VarHandle;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Reads a {@link SequencedRingBuffer} without holding its producer back: unlike a {@link SequencedPoller} it adds no
 * gating sequence, so a reader that falls a whole ring behind is lapped rather than making the producer wait. Events
 * it was lapped past are skipped and counted ({@link #lapped()}). For a reader whose job matters less than the
 * producer's, such as a shard sending a copy of what it collects to whoever is listening.
 *
 * <p>Each event is copied out of its slot and then checked like a seqlock read: if the producer may have claimed the
 * slot again while it was being copied, the copy is discarded and counted as lapped. Handlers get the copy, valid
 * until the next event. One thread polls; zero allocation.
 */
public final class LappingPoller {

    private final RingBuffer<SequencedEvent> ring;
    private final int size;
    private final SequencedEventHandler handler;
    private final UnsafeBuffer copy = ByteBufferUtils.createAlignedUnsafeBuffer(SequencedEvent.MAX_MESSAGE_SIZE);
    private long next;
    private long lapped;

    LappingPoller(final RingBuffer<SequencedEvent> ring, final SequencedEventHandler handler) {
        this.ring = ring;
        this.size = ring.getBufferSize();
        this.handler = handler;
        this.next = ring.getCursor() + 1;
    }

    /** Hands every event published since the last poll to the handler, skipping any it was lapped past. */
    public int poll() throws Exception {
        final long claimed = ring.getCursor();
        if (claimed < next) {
            return 0;
        }
        if (claimed - next >= size) {
            final long oldestIntact = claimed - size + 1;
            lapped += oldestIntact - next;
            next = oldestIntact;
        }
        final long available = publishedUpTo(claimed);
        int count = 0;
        for (long sequence = next; sequence <= available; sequence++) {
            if (read(sequence)) {
                count++;
            } else {
                lapped++;
            }
        }
        next = Math.max(next, available + 1);
        return count;
    }

    /** Claimed isn't published: with producers taking turns a slot is claimed before it is written. */
    private long publishedUpTo(final long claimed) {
        long available = next - 1;
        while (available < claimed && ring.isAvailable(available + 1)) {
            available++;
        }
        return available;
    }

    /** Copies the slot and hands it on; false when the producer may have rewritten it during the copy. */
    private boolean read(final long sequence) throws Exception {
        final SequencedEvent event = ring.get(sequence);
        final long globalSequence = event.globalSequence;
        final int templateId = event.templateId;
        final int length = event.bufferLength;
        if (length < 0 || length > SequencedEvent.MAX_MESSAGE_SIZE) {
            return false;
        }
        copy.putBytes(0, event.buffer, 0, length);
        // The slot is rewritten only once its sequence plus the ring size is claimed: checked after the copy, and
        // after a fence so the copy's reads can't drift past the check.
        VarHandle.loadLoadFence();
        if (ring.getCursor() - sequence >= size) {
            return false;
        }
        handler.onSequencedEvent(globalSequence, templateId, copy, length);
        return true;
    }

    /** Events skipped because the producer lapped this reader. */
    public long lapped() {
        return lapped;
    }
}
