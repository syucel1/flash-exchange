package com.flashexchange.engine.config;

import java.util.Properties;

/**
 * Journal, snapshot and archive settings.
 *
 * @param journalEnabled                 journal every state-changing command to the Aeron Archive (stream 100) and
 *                                       recover from snapshot + journal on start.
 * @param dataDirectory                  node data directory (archive files and the epoch file).
 * @param archiveHost                    address the archive control channel binds to (remote access by the peer).
 * @param archiveControlPort             UDP port of the archive control channel; 0 = local (IPC) access only.
 * @param fileSyncLevel                  archive fsync level: 0 = page cache, 1 = data, 2 = data + metadata.
 * @param journalTermLength              term length of the journal publication (power of two, &gt;= 64 KiB).
 * @param stateSnapshotIntervalMs        take a state snapshot at this period (0 = disabled).
 * @param stateSnapshotIntervalCommands  take a state snapshot after this many journaled commands (0 = disabled).
 * @param snapshotsRetained              number of complete snapshots kept; older ones are purged.
 * @param dedupWindow                    recent (user, client order id) pairs checked for duplicates (0 = off).
 */
public record DurabilityConfig(
    boolean journalEnabled,
    String dataDirectory,
    String archiveHost,
    int archiveControlPort,
    int fileSyncLevel,
    int journalTermLength,
    long stateSnapshotIntervalMs,
    long stateSnapshotIntervalCommands,
    int snapshotsRetained,
    int dedupWindow)
{
    /** Default journal term length (16 MiB). */
    public static final int DEFAULT_JOURNAL_TERM_LENGTH = 16 * 1024 * 1024;

    /**
     * Validate.
     */
    public DurabilityConfig
    {
        if (journalEnabled && (dataDirectory == null || dataDirectory.isBlank()))
        {
            throw new IllegalArgumentException("dataDirectory is required when the journal is enabled");
        }
        if (archiveControlPort < 0 || archiveControlPort > 65535)
        {
            throw new IllegalArgumentException("archiveControlPort out of range: " + archiveControlPort);
        }
        if (fileSyncLevel < 0 || fileSyncLevel > 2)
        {
            throw new IllegalArgumentException("fileSyncLevel must be 0, 1 or 2: " + fileSyncLevel);
        }
        if (journalTermLength < 65536 || Integer.bitCount(journalTermLength) != 1)
        {
            throw new IllegalArgumentException("journalTermLength must be a power of two >= 64KiB");
        }
        if (stateSnapshotIntervalMs < 0 || stateSnapshotIntervalCommands < 0 || snapshotsRetained < 1 ||
            dedupWindow < 0)
        {
            throw new IllegalArgumentException("snapshot intervals and dedup window must be >= 0, retained >= 1");
        }
    }

    /**
     * Durability disabled (in-memory engine), with duplicate detection on.
     *
     * @return defaults.
     */
    public static DurabilityConfig disabled()
    {
        return new DurabilityConfig(false, "data", "localhost", 0, 0, DEFAULT_JOURNAL_TERM_LENGTH, 60_000,
            100_000, 3, 65_536);
    }

    /**
     * Journal enabled in a data directory, with default intervals.
     *
     * @param dataDirectory data directory.
     * @return configuration.
     */
    public static DurabilityConfig journal(final String dataDirectory)
    {
        return disabled().withJournal(true, dataDirectory);
    }

    /**
     * Copy with the journal switch and data directory changed.
     *
     * @param enabled       journal on/off.
     * @param directory     data directory.
     * @return copy.
     */
    public DurabilityConfig withJournal(final boolean enabled, final String directory)
    {
        return new DurabilityConfig(enabled, directory, archiveHost, archiveControlPort, fileSyncLevel,
            journalTermLength, stateSnapshotIntervalMs, stateSnapshotIntervalCommands, snapshotsRetained,
            dedupWindow);
    }

    /**
     * Copy with the archive control endpoint changed.
     *
     * @param host address.
     * @param port UDP port (0 = local only).
     * @return copy.
     */
    public DurabilityConfig withArchiveControl(final String host, final int port)
    {
        return new DurabilityConfig(journalEnabled, dataDirectory, host, port, fileSyncLevel, journalTermLength,
            stateSnapshotIntervalMs, stateSnapshotIntervalCommands, snapshotsRetained, dedupWindow);
    }

    /**
     * Copy with the snapshot policy changed.
     *
     * @param intervalMs       period (0 = off).
     * @param intervalCommands commands between snapshots (0 = off).
     * @return copy.
     */
    public DurabilityConfig withSnapshotInterval(final long intervalMs, final long intervalCommands)
    {
        return new DurabilityConfig(journalEnabled, dataDirectory, archiveHost, archiveControlPort, fileSyncLevel,
            journalTermLength, intervalMs, intervalCommands, snapshotsRetained, dedupWindow);
    }

    /**
     * Copy with the dedup window changed.
     *
     * @param window pairs remembered (0 = off).
     * @return copy.
     */
    public DurabilityConfig withDedupWindow(final int window)
    {
        return new DurabilityConfig(journalEnabled, dataDirectory, archiveHost, archiveControlPort, fileSyncLevel,
            journalTermLength, stateSnapshotIntervalMs, stateSnapshotIntervalCommands, snapshotsRetained, window);
    }

    /**
     * Copy with another journal term length.
     *
     * @param termLength power of two &gt;= 64KiB.
     * @return new configuration.
     */
    public DurabilityConfig withJournalTermLength(final int termLength)
    {
        return new DurabilityConfig(journalEnabled, dataDirectory, archiveHost, archiveControlPort, fileSyncLevel,
            termLength, stateSnapshotIntervalMs, stateSnapshotIntervalCommands, snapshotsRetained, dedupWindow);
    }

    /**
     * Read {@code flash.engine.journal.*}, {@code archive.*}, {@code snapshot.state.*}, {@code data.dir} and
     * {@code dedup.window} keys.
     *
     * @param p      properties.
     * @param prefix key prefix.
     * @return configuration.
     */
    public static DurabilityConfig fromProperties(final Properties p, final String prefix)
    {
        final DurabilityConfig d = disabled();
        return new DurabilityConfig(
            Boolean.parseBoolean(get(p, prefix, "journal.enabled", String.valueOf(d.journalEnabled))),
            get(p, prefix, "data.dir", d.dataDirectory),
            get(p, prefix, "archive.host", d.archiveHost),
            Integer.parseInt(get(p, prefix, "archive.controlPort", String.valueOf(d.archiveControlPort))),
            Integer.parseInt(get(p, prefix, "archive.fileSyncLevel", String.valueOf(d.fileSyncLevel))),
            Integer.parseInt(get(p, prefix, "journal.termLength", String.valueOf(d.journalTermLength))),
            Long.parseLong(get(p, prefix, "snapshot.state.intervalMs", String.valueOf(d.stateSnapshotIntervalMs))),
            Long.parseLong(get(p, prefix, "snapshot.state.intervalCommands",
                String.valueOf(d.stateSnapshotIntervalCommands))),
            Integer.parseInt(get(p, prefix, "snapshot.state.retained", String.valueOf(d.snapshotsRetained))),
            Integer.parseInt(get(p, prefix, "dedup.window", String.valueOf(d.dedupWindow))));
    }

    static String get(final Properties p, final String prefix, final String key, final String defaultValue)
    {
        final String value = p.getProperty(prefix + key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
