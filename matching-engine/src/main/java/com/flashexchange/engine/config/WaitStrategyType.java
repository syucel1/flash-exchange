package com.flashexchange.engine.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

/**
 * Disruptor wait strategy used by the pipeline consumers. Lower latency costs more CPU.
 */
public enum WaitStrategyType
{
    /** Spin on dedicated cores: lowest latency, one full core per consumer. Production choice. */
    BUSY_SPIN,

    /** Spin then yield: low latency with some CPU relief. */
    YIELDING,

    /** Spin, yield, then park briefly: moderate latency, low CPU. Good default for development. */
    SLEEPING,

    /** Lock and condition: highest latency, lowest CPU. */
    BLOCKING;

    /**
     * Create a new strategy instance.
     *
     * @return the wait strategy.
     */
    public WaitStrategy create()
    {
        return switch (this)
        {
            case BUSY_SPIN -> new BusySpinWaitStrategy();
            case YIELDING -> new YieldingWaitStrategy();
            case SLEEPING -> new SleepingWaitStrategy();
            case BLOCKING -> new BlockingWaitStrategy();
        };
    }
}
