package com.flashexchange.engine.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EpochFileTest
{
    @TempDir
    Path tempDir;

    @Test
    void readsDefaultWritesAndReadsBack() throws Exception
    {
        final EpochFile file = new EpochFile(tempDir.resolve("sub/epoch"));
        assertEquals(-5, file.read(-5));
        file.write(42);
        assertEquals(42, file.read(0));
        file.write(43);
        assertEquals(43, new EpochFile(file.path()).read(0));
        assertEquals("43", Files.readString(file.path()));
    }

    @Test
    void ioErrorsAreUnchecked() throws Exception
    {
        final Path dir = tempDir.resolve("dir");
        Files.createDirectories(dir.resolve("epoch"));
        final EpochFile file = new EpochFile(dir.resolve("epoch"));
        assertThrows(UncheckedIOException.class, () -> file.read(0));
        assertThrows(UncheckedIOException.class, () -> file.write(1));
    }
}
