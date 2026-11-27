package com.flashexchange.engine.pipeline;

import com.flashexchange.protocol.EngineRoleFlyweight;
import com.flashexchange.protocol.NodeRole;
import com.lmax.disruptor.EventHandler;
import org.agrona.ExpandableArrayBuffer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Final Disruptor stage: the release gate and egress. For each slot, in engine sequence order, it waits until the
 * command may be released (see {@link ReleaseControl}), then publishes the encoded output to the execution and
 * market data publishers and clears the slot.
 * <p>
 * Output handling depends on the node role, switched by {@link CommandType#ROLE_CHANGE} slots so that the switch
 * happens at an exact point of the output stream:
 * <ul>
 *   <li>STANDALONE / PRIMARY: publish.</li>
 *   <li>STANDBY: retain (do not publish) until the primary reports the output released; acknowledge each batch.</li>
 *   <li>FENCED: drop everything; another node owns the output stream.</li>
 * </ul>
 * Every role change is announced to consumers with an {@code EngineRole} message on both streams. On promotion the
 * retained output is published right after the announcement.
 */
public final class PublisherEventHandler implements EventHandler<OrderCommand>
{
    /**
     * Sequences a standby keeps beyond what the primary reported released. "Released" means handed to the primary's
     * Aeron publication, not delivered: a primary that dies can lose output still buffered in its driver, so the
     * standby re-publishes this window on promotion and consumers drop what they already have.
     */
    public static final long RETAIN_WINDOW = 16_384;

    private final MessagePublisher executionPublisher;
    private final MessagePublisher marketDataPublisher;
    private final ReleaseControl releaseControl;
    private final int nodeId;
    private final RetainedOutput retained = new RetainedOutput();
    private final ExpandableArrayBuffer roleBuffer = new ExpandableArrayBuffer(64);
    private final EngineRoleFlyweight roleMessage = new EngineRoleFlyweight();
    private volatile long lastPublishedSequence = -1;
    private volatile long lastReleasedEngineSequence = -1;
    private volatile long lastHandledEngineSequence = -1;
    private volatile NodeRole role = NodeRole.STANDALONE;
    private final AtomicLong retainedRepublished = new AtomicLong();

    /**
     * Create the publisher stage without durability (in-memory engine).
     *
     * @param executionPublisher  destination of private events (stream 2001).
     * @param marketDataPublisher destination of market data (stream 3001).
     */
    public PublisherEventHandler(
        final MessagePublisher executionPublisher, final MessagePublisher marketDataPublisher)
    {
        this(executionPublisher, marketDataPublisher, null, 0);
    }

    /**
     * Create the publisher stage.
     *
     * @param executionPublisher  destination of private events (stream 2001).
     * @param marketDataPublisher destination of market data (stream 3001).
     * @param releaseControl      release gate and snapshot writer, or {@code null}.
     * @param nodeId              node id announced in role messages.
     */
    public PublisherEventHandler(
        final MessagePublisher executionPublisher,
        final MessagePublisher marketDataPublisher,
        final ReleaseControl releaseControl,
        final int nodeId)
    {
        this.executionPublisher = executionPublisher;
        this.marketDataPublisher = marketDataPublisher;
        this.releaseControl = releaseControl;
        this.nodeId = nodeId;
    }

    @Override
    public void onEvent(final OrderCommand command, final long sequence, final boolean endOfBatch)
    {
        try
        {
            final CommandType type = command.type;
            if (type == CommandType.ROLE_CHANGE)
            {
                onRoleChange(command);
            }
            else if (type == CommandType.STATE_SNAPSHOT)
            {
                if (releaseControl != null)
                {
                    releaseControl.onStateSnapshot(command);
                }
            }
            else
            {
                release(command);
            }
        }
        finally
        {
            command.clearOutput();
            lastPublishedSequence = sequence;
        }

        if (endOfBatch && releaseControl != null)
        {
            releaseControl.onBatchReleased(lastHandledEngineSequence, role == NodeRole.STANDBY);
        }
    }

    private void release(final OrderCommand command)
    {
        final NodeRole currentRole = role;
        final boolean retaining = currentRole == NodeRole.STANDBY;
        final boolean mayPublish = releaseControl == null || releaseControl.awaitRelease(command, retaining);
        final boolean journaled = command.type != null && command.type.isJournaled();
        if (retaining)
        {
            retained.retain(command.engineSequence, command);
            retained.trim(command.releasedSequence - RETAIN_WINDOW);
        }
        else if (mayPublish && currentRole != NodeRole.FENCED)
        {
            command.executionOutput().drainTo(executionPublisher);
            command.marketDataOutput().drainTo(marketDataPublisher);
            if (journaled)
            {
                lastReleasedEngineSequence = command.engineSequence;
            }
        }
        if (journaled)
        {
            lastHandledEngineSequence = command.engineSequence;
        }
    }

    private void onRoleChange(final OrderCommand command)
    {
        final NodeRole newRole = command.role;
        final NodeRole oldRole = role;
        role = newRole;
        if (newRole == NodeRole.FENCED)
        {
            retained.clear();
            return;
        }
        if (newRole == NodeRole.STANDBY)
        {
            return;
        }
        roleMessage.wrapForEncode(roleBuffer, 0)
            .epoch(command.epoch)
            .lastSequenceBefore(command.engineSequence)
            .timestamp(command.timestamp)
            .nodeId(nodeId)
            .role(newRole);
        executionPublisher.publish(roleBuffer, 0, roleMessage.encodedLength());
        marketDataPublisher.publish(roleBuffer, 0, roleMessage.encodedLength());
        if (oldRole == NodeRole.STANDBY)
        {
            retainedRepublished.addAndGet(retained.drainTo(executionPublisher, marketDataPublisher));
            lastReleasedEngineSequence = lastHandledEngineSequence;
        }
    }

    /**
     * Ring sequence of the last slot handled (for monitoring and orderly shutdown).
     *
     * @return last ring sequence, -1 before the first.
     */
    public long lastPublishedSequence()
    {
        return lastPublishedSequence;
    }

    /**
     * Highest engine sequence whose output was published.
     *
     * @return sequence, -1 before the first.
     */
    public long lastReleasedEngineSequence()
    {
        return lastReleasedEngineSequence;
    }

    /**
     * Highest engine sequence handled (published or retained).
     *
     * @return sequence, -1 before the first.
     */
    public long lastHandledEngineSequence()
    {
        return lastHandledEngineSequence;
    }

    /**
     * Current output role.
     *
     * @return role.
     */
    public NodeRole role()
    {
        return role;
    }

    /**
     * Retained messages (standby).
     *
     * @return count.
     */
    public int retainedMessages()
    {
        return retained.size();
    }

    /**
     * Messages re-published from the retained output on promotion.
     *
     * @return count.
     */
    public long retainedRepublished()
    {
        return retainedRepublished.get();
    }
}
