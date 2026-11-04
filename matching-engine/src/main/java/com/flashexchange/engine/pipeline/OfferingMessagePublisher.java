package com.flashexchange.engine.pipeline;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.NOT_CONNECTED;

/**
 * {@link MessagePublisher} over an Aeron publication with an explicit back-pressure policy:
 * <ul>
 *   <li>{@code BACK_PRESSURED} / {@code ADMIN_ACTION}: retry with the idle strategy until {@code retryTimeoutNs}
 *       elapses, then drop and count ({@link #droppedBackPressured()});</li>
 *   <li>{@code NOT_CONNECTED}: no subscriber is listening; drop immediately and count
 *       ({@link #droppedNotConnected()});</li>
 *   <li>{@code CLOSED} / {@code MAX_POSITION_EXCEEDED} / anything else: drop and count ({@link #failed()}).</li>
 * </ul>
 * Only the single publisher thread calls {@link #publish(DirectBuffer, int, int)}; counters may be read from any
 * thread.
 */
public final class OfferingMessagePublisher implements MessagePublisher
{
    /**
     * Seam over {@code Publication#offer(DirectBuffer, int, int)} so the policy can be tested without a driver.
     */
    @FunctionalInterface
    public interface OfferTarget
    {
        /**
         * Offer a message.
         *
         * @param buffer containing the message.
         * @param offset of the message.
         * @param length of the message.
         * @return new stream position (&gt; 0) or a negative Aeron publication result code.
         */
        long offer(DirectBuffer buffer, int offset, int length);
    }

    private final String name;
    private final OfferTarget target;
    private final IdleStrategy idleStrategy;
    private final NanoClock clock;
    private final long retryTimeoutNs;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong droppedNotConnected = new AtomicLong();
    private final AtomicLong droppedBackPressured = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    /**
     * Create a publisher.
     *
     * @param name           used in diagnostics.
     * @param target         publication to offer to.
     * @param idleStrategy   used between retries.
     * @param clock          for the retry timeout.
     * @param retryTimeoutNs how long to retry a back-pressured offer before dropping.
     */
    public OfferingMessagePublisher(
        final String name,
        final OfferTarget target,
        final IdleStrategy idleStrategy,
        final NanoClock clock,
        final long retryTimeoutNs)
    {
        this.name = name;
        this.target = target;
        this.idleStrategy = idleStrategy;
        this.clock = clock;
        this.retryTimeoutNs = retryTimeoutNs;
    }

    @Override
    public boolean publish(final DirectBuffer buffer, final int offset, final int length)
    {
        long deadlineNs = 0;
        idleStrategy.reset();
        while (true)
        {
            final long result = target.offer(buffer, offset, length);
            if (result > 0)
            {
                published.lazySet(published.get() + 1);
                return true;
            }

            if (result == BACK_PRESSURED || result == ADMIN_ACTION)
            {
                final long nowNs = clock.nanoTime();
                if (deadlineNs == 0)
                {
                    deadlineNs = nowNs + retryTimeoutNs;
                }
                else if (nowNs - deadlineNs >= 0)
                {
                    droppedBackPressured.lazySet(droppedBackPressured.get() + 1);
                    return false;
                }
                idleStrategy.idle();
            }
            else if (result == NOT_CONNECTED)
            {
                droppedNotConnected.lazySet(droppedNotConnected.get() + 1);
                return false;
            }
            else
            {
                failed.lazySet(failed.get() + 1);
                return false;
            }
        }
    }

    /**
     * Name of the publisher.
     *
     * @return name.
     */
    public String name()
    {
        return name;
    }

    /**
     * Messages successfully offered.
     *
     * @return count.
     */
    public long published()
    {
        return published.get();
    }

    /**
     * Messages dropped because no subscriber was connected.
     *
     * @return count.
     */
    public long droppedNotConnected()
    {
        return droppedNotConnected.get();
    }

    /**
     * Messages dropped after back-pressure outlasted the retry timeout.
     *
     * @return count.
     */
    public long droppedBackPressured()
    {
        return droppedBackPressured.get();
    }

    /**
     * Messages dropped because the publication was closed or failed.
     *
     * @return count.
     */
    public long failed()
    {
        return failed.get();
    }

    @Override
    public String toString()
    {
        return name + "{published=" + published() + ", droppedNotConnected=" + droppedNotConnected() +
            ", droppedBackPressured=" + droppedBackPressured() + ", failed=" + failed() + "}";
    }
}
