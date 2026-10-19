package group.gnometrading.sequencer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import org.agrona.IoUtil;

/**
 * One finished file of a journal. Until {@link #seal} it keeps its full mapped size: only its first
 * {@link #writtenBytes()} hold records, and the rest is zeros, where a reader stops.
 *
 * <p>It stays mapped until sealed. A deleted file that is still mapped keeps its disk space, and nothing else unmaps
 * it in a process that never collects garbage.
 */
public final class JournalSegment {

    private final int index;
    private final int writtenBytes;
    private final long firstSequence;
    private final long lastSequence;
    private MappedByteBuffer mapping;
    private Path path;

    JournalSegment(
            int index, Path path, int writtenBytes, long firstSequence, long lastSequence, MappedByteBuffer mapping) {
        this.index = index;
        this.path = path;
        this.writtenBytes = writtenBytes;
        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
        this.mapping = mapping;
    }

    /**
     * Unmaps the segment, trims its file to the records, and renames it to {@link JournalWriter#sealedFileName}: the
     * rename tells whatever ships the journal that the file is complete. Not forced to disk: the pages are in the
     * page cache, which outlives the process, and a lost instance loses the disk anyway. Unmapping can take a
     * millisecond or more, so not for the writing thread. Sealing it twice does nothing.
     *
     * @throws UncheckedIOException if the file cannot be trimmed or renamed
     */
    public void seal() {
        if (mapping == null) {
            return;
        }
        IoUtil.unmap(mapping);
        mapping = null;
        try {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
                channel.truncate(writtenBytes);
            }
            path = Files.move(
                    path, path.resolveSibling(JournalWriter.sealedFileName(index)), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to seal journal segment " + path, e);
        }
    }

    /** The segment's position in the journal, from 0. */
    public int index() {
        return index;
    }

    /** Its file: the sealed one once {@link #seal} has run. */
    public Path path() {
        return path;
    }

    /** How many bytes of the file hold records. */
    public int writtenBytes() {
        return writtenBytes;
    }

    /** The global sequence of its first record. */
    public long firstSequence() {
        return firstSequence;
    }

    /** The global sequence of its last record. */
    public long lastSequence() {
        return lastSequence;
    }
}
