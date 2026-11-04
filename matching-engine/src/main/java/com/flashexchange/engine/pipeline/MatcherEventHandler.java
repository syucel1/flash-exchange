package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.matching.MatchingEngine;
import com.lmax.disruptor.EventHandler;

/**
 * Disruptor stage that runs one matching shard. Instruments are partitioned across shards by
 * {@code floorMod(instrumentId, shardCount)}; each shard owns its books exclusively, so there is exactly one writer
 * per book and no locking. Commands for other shards are skipped.
 */
public final class MatcherEventHandler implements EventHandler<OrderCommand>
{
    private final int shardId;
    private final int shardCount;
    private final MatchingEngine engine;
    private final EncodingEventSink sink;

    /**
     * Create a matcher stage.
     *
     * @param shardId    of this stage (0 .. shardCount-1).
     * @param shardCount total number of matcher stages.
     * @param engine     matching engine holding the instruments of this shard; must emit into {@code sink}.
     * @param sink       encoder bound to each slot before processing.
     */
    public MatcherEventHandler(
        final int shardId, final int shardCount, final MatchingEngine engine, final EncodingEventSink sink)
    {
        if (shardCount < 1 || shardId < 0 || shardId >= shardCount)
        {
            throw new IllegalArgumentException("invalid shard " + shardId + " of " + shardCount);
        }
        this.shardId = shardId;
        this.shardCount = shardCount;
        this.engine = engine;
        this.sink = sink;
    }

    /**
     * Shard that owns an instrument.
     *
     * @param instrumentId instrument.
     * @param shardCount   number of shards.
     * @return owning shard.
     */
    public static int shardOf(final int instrumentId, final int shardCount)
    {
        return Math.floorMod(instrumentId, shardCount);
    }

    @Override
    public void onEvent(final OrderCommand command, final long sequence, final boolean endOfBatch)
    {
        if (shardOf(command.instrumentId, shardCount) != shardId)
        {
            return;
        }

        sink.bind(command.executionOutput(), command.marketDataOutput());
        switch (command.type)
        {
            case NEW_ORDER:
                engine.onNewOrder(
                    command.engineSequence, command.timestamp, command.orderId, command.clientOrderId,
                    command.userId, command.instrumentId, command.side, command.orderType, command.timeInForce,
                    command.selfTradePrevention, command.postOnly, command.price, command.quantity);
                break;

            case CANCEL_ORDER:
                engine.onCancelOrder(
                    command.engineSequence, command.timestamp, command.clientOrderId, command.userId,
                    command.instrumentId, command.orderId);
                break;

            case BOOK_SNAPSHOT:
                engine.onBookSnapshotRequest(command.engineSequence, command.timestamp, command.instrumentId);
                break;

            default:
                throw new IllegalStateException("unknown command type: " + command.type);
        }
    }
}
