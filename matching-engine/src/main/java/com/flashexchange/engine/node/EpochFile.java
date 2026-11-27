package com.flashexchange.engine.node;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * A single {@code long} persisted atomically in a small text file (write to a temporary file, then rename). Used
 * for the node epoch and by the arbiter for its election state.
 */
public final class EpochFile
{
    private final Path file;

    /**
     * Create a handle.
     *
     * @param file location; the parent directory is created on first write.
     */
    public EpochFile(final Path file)
    {
        this.file = file;
    }

    /**
     * Read the stored value.
     *
     * @param defaultValue returned when the file does not exist.
     * @return stored value.
     */
    public long read(final long defaultValue)
    {
        if (!Files.exists(file))
        {
            return defaultValue;
        }
        try
        {
            return Long.parseLong(Files.readString(file, StandardCharsets.UTF_8).trim());
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Store a value atomically.
     *
     * @param value to store.
     */
    public void write(final long value)
    {
        try
        {
            final Path parent = file.toAbsolutePath().getParent();
            if (parent == null)
            {
                throw new IOException("epoch file has no parent directory: " + file);
            }
            Files.createDirectories(parent);
            final Path tmp = parent.resolve(file.getFileName() + ".tmp");
            Files.writeString(tmp, Long.toString(value), StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Location.
     *
     * @return path.
     */
    public Path path()
    {
        return file;
    }
}
