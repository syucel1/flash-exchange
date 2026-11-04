package com.flashexchange.engine.pipeline;

import com.lmax.disruptor.ExceptionHandler;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Disruptor exception handler that logs and counts failures of individual commands and lets the pipeline continue,
 * so one poison command cannot halt the engine. Lifecycle failures are logged only.
 */
public final class CountingExceptionHandler implements ExceptionHandler<OrderCommand>
{
    private static final System.Logger LOGGER = System.getLogger(CountingExceptionHandler.class.getName());

    private final AtomicLong eventExceptions = new AtomicLong();

    @Override
    public void handleEventException(final Throwable ex, final long sequence, final OrderCommand command)
    {
        eventExceptions.incrementAndGet();
        LOGGER.log(System.Logger.Level.ERROR, "error processing sequence " + sequence + " type=" +
            (command == null ? null : command.type()), ex);
    }

    @Override
    public void handleOnStartException(final Throwable ex)
    {
        LOGGER.log(System.Logger.Level.ERROR, "error starting pipeline handler", ex);
    }

    @Override
    public void handleOnShutdownException(final Throwable ex)
    {
        LOGGER.log(System.Logger.Level.ERROR, "error stopping pipeline handler", ex);
    }

    /**
     * Number of commands whose processing threw.
     *
     * @return count.
     */
    public long eventExceptions()
    {
        return eventExceptions.get();
    }
}
