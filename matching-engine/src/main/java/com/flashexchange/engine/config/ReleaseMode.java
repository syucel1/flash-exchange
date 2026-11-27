package com.flashexchange.engine.config;

/**
 * When the primary may release (publish) the output of a command.
 */
public enum ReleaseMode
{
    /** After the command is durable in the local journal and acknowledged by the standby (RPO 0). */
    SYNC,

    /** After the command is durable in the local journal; replication to the standby is asynchronous. */
    ASYNC
}
