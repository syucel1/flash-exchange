package com.flashexchange.engine.pipeline;

import com.lmax.disruptor.EventHandler;

/**
 * Final Disruptor stage: publishes the encoded output of each command, in engine sequence order, to the execution
 * and market data publishers and clears the slot for reuse.
 */
public final class PublisherEventHandler implements EventHandler<OrderCommand>
{
    private final MessagePublisher executionPublisher;
    private final MessagePublisher marketDataPublisher;
    private volatile long lastPublishedSequence = -1;

    /**
     * Create the publisher stage.
     *
     * @param executionPublisher  destination of private events (stream 2001).
     * @param marketDataPublisher destination of market data (stream 3001).
     */
    public PublisherEventHandler(
        final MessagePublisher executionPublisher, final MessagePublisher marketDataPublisher)
    {
        this.executionPublisher = executionPublisher;
        this.marketDataPublisher = marketDataPublisher;
    }

    @Override
    public void onEvent(final OrderCommand command, final long sequence, final boolean endOfBatch)
    {
        try
        {
            command.executionOutput().drainTo(executionPublisher);
            command.marketDataOutput().drainTo(marketDataPublisher);
        }
        finally
        {
            command.clearOutput();
            lastPublishedSequence = sequence;
        }
    }

    /**
     * Sequence of the last slot handled (for monitoring and orderly shutdown).
     *
     * @return last sequence, -1 before the first.
     */
    public long lastPublishedSequence()
    {
        return lastPublishedSequence;
    }
}
