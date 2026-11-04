package com.flashexchange.engine.config;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

/**
 * Agrona idle strategy used by polling agents (the Aeron ingress poller) when there is no work.
 */
public enum IdleStrategyType
{
    /** Busy spin with a CPU pause hint. */
    BUSY_SPIN,

    /** No-op: hot spin without pause hint. */
    NO_OP,

    /** Thread yield. */
    YIELDING,

    /** Spin, yield, then park with exponential back-off. Good general default. */
    BACKOFF,

    /** Sleep 1 ms. */
    SLEEPING;

    /**
     * Create a new strategy instance.
     *
     * @return the idle strategy.
     */
    public IdleStrategy create()
    {
        return switch (this)
        {
            case BUSY_SPIN -> new BusySpinIdleStrategy();
            case NO_OP -> new NoOpIdleStrategy();
            case YIELDING -> new YieldingIdleStrategy();
            case BACKOFF -> new BackoffIdleStrategy();
            case SLEEPING -> new SleepingMillisIdleStrategy(1);
        };
    }
}
