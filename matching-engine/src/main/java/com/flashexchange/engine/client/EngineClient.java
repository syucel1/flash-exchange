package com.flashexchange.engine.client;

import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.CancelOrderFlyweight;
import com.flashexchange.protocol.EngineEventListener;
import com.flashexchange.protocol.EngineRoleFlyweight;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.EngineMessageDispatcher;
import com.flashexchange.protocol.NewOrderFlyweight;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client side of the engine's Aeron interface, for components such as the trading service (and the integration
 * tests): sends commands on the command stream and polls execution events and market data.
 * <p>
 * Not thread safe: use one instance per thread (the command publication itself is concurrent, so several clients
 * may share an {@link Aeron} instance).
 */
public final class EngineClient implements AutoCloseable
{
    private static final String MANUAL_MDC = "aeron:udp?control-mode=manual";

    private final Publication commandPublication;
    private final Subscription[] executionSubscriptions;
    private final Subscription[] marketDataSubscriptions;
    private final OutputDeduplicator executionDedup = new OutputDeduplicator();
    private final OutputDeduplicator marketDataDedup = new OutputDeduplicator();
    private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(256);
    private final NewOrderFlyweight newOrder = new NewOrderFlyweight();
    private final CancelOrderFlyweight cancelOrder = new CancelOrderFlyweight();
    private final BookSnapshotRequestFlyweight snapshotRequest = new BookSnapshotRequestFlyweight();
    private final EngineMessageDispatcher dispatcher = new EngineMessageDispatcher();
    private final FragmentAssembler[] executionAssemblers;
    private final FragmentAssembler[] marketDataAssemblers;
    private final long[] executionEpochs;
    private final long[] marketDataEpochs;
    private long staleDropped;
    private final long sendTimeoutNs;
    private EngineEventListener listener;

    /**
     * Create a client of one engine.
     *
     * @param aeron         connected Aeron client.
     * @param channels      client-side channels of the engine.
     * @param sendTimeoutMs how long a send may retry while back-pressured or not connected.
     */
    public EngineClient(final Aeron aeron, final EngineChannels channels, final long sendTimeoutMs)
    {
        this(aeron, List.of(channels), sendTimeoutMs);
    }

    /**
     * Create a client of an HA engine pair (or any set of engines). Commands go to every engine through one
     * multi-destination publication: only the primary acts on them, standbys discard them. Output is received from
     * every engine and de-duplicated across failovers (see {@link OutputDeduplicator}).
     *
     * @param aeron         connected Aeron client.
     * @param engines       client-side channels of each engine; command channels must be UDP for more than one.
     * @param sendTimeoutMs how long a send may retry while back-pressured or not connected.
     */
    public EngineClient(final Aeron aeron, final List<EngineChannels> engines, final long sendTimeoutMs)
    {
        if (engines.isEmpty())
        {
            throw new IllegalArgumentException("at least one engine is required");
        }
        final EngineChannels first = engines.get(0);
        if (engines.size() == 1)
        {
            this.commandPublication = aeron.addPublication(first.commandChannel(), first.commandStreamId());
        }
        else
        {
            this.commandPublication = aeron.addExclusivePublication(MANUAL_MDC, first.commandStreamId());
            for (final EngineChannels engine : engines)
            {
                commandPublication.addDestination(engine.commandChannel());
            }
        }
        this.executionSubscriptions = new Subscription[engines.size()];
        this.marketDataSubscriptions = new Subscription[engines.size()];
        for (int i = 0; i < engines.size(); i++)
        {
            final EngineChannels engine = engines.get(i);
            executionSubscriptions[i] = aeron.addSubscription(engine.executionChannel(), engine.executionStreamId());
            marketDataSubscriptions[i] =
                aeron.addSubscription(engine.marketDataChannel(), engine.marketDataStreamId());
        }
        this.sendTimeoutNs = TimeUnit.MILLISECONDS.toNanos(sendTimeoutMs);
        this.executionAssemblers = new FragmentAssembler[engines.size()];
        this.marketDataAssemblers = new FragmentAssembler[engines.size()];
        this.executionEpochs = new long[engines.size()];
        this.marketDataEpochs = new long[engines.size()];
        Arrays.fill(executionEpochs, Long.MIN_VALUE);
        Arrays.fill(marketDataEpochs, Long.MIN_VALUE);
        for (int i = 0; i < engines.size(); i++)
        {
            final int index = i;
            executionAssemblers[i] = new FragmentAssembler((buffer, offset, length, header) ->
                onMessage(buffer, offset, length, index, executionEpochs, executionDedup));
            marketDataAssemblers[i] = new FragmentAssembler((buffer, offset, length, header) ->
                onMessage(buffer, offset, length, index, marketDataEpochs, marketDataDedup));
        }
    }

    private void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int engineIndex,
        final long[] epochs,
        final OutputDeduplicator dedup)
    {
        if (length >= EngineRoleFlyweight.ENCODED_LENGTH &&
            MessageHeader.templateId(buffer, offset) == EngineRoleFlyweight.TEMPLATE_ID)
        {
            final long epoch = buffer.getLong(offset + MessageHeader.LENGTH, MessageHeader.BYTE_ORDER);
            epochs[engineIndex] = Math.max(epochs[engineIndex], epoch);
        }
        // output of an engine that has been superseded (its epoch is older than the newest announced) is stale
        long newest = Long.MIN_VALUE;
        for (final long epoch : epochs)
        {
            newest = Math.max(newest, epoch);
        }
        if (epochs[engineIndex] < newest)
        {
            staleDropped++;
            return;
        }
        if (dedup.accept(buffer, offset, length))
        {
            dispatcher.dispatch(buffer, offset, length, listener);
        }
    }

    /**
     * Whether the command publication and both subscriptions are connected to the engine.
     *
     * @return true when ready.
     */
    public boolean isConnected()
    {
        return commandPublication.isConnected() && anyConnected(executionSubscriptions) &&
            anyConnected(marketDataSubscriptions);
    }

    private static boolean anyConnected(final Subscription[] subscriptions)
    {
        for (final Subscription subscription : subscriptions)
        {
            if (subscription.isConnected())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Wait until {@link #isConnected()} or the timeout elapses.
     *
     * @param timeoutMs maximum wait.
     * @return true if connected.
     */
    public boolean awaitConnected(final long timeoutMs)
    {
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        final IdleStrategy idle = new SleepingMillisIdleStrategy(1);
        while (!isConnected())
        {
            if (System.nanoTime() - deadlineNs >= 0)
            {
                return false;
            }
            idle.idle();
        }
        return true;
    }

    /**
     * Send a new order.
     *
     * @param clientOrderId       client order id.
     * @param userId              user.
     * @param instrumentId        instrument.
     * @param side                side.
     * @param orderType           order type.
     * @param timeInForce         time in force.
     * @param price               limit price (0 for market).
     * @param quantity            quantity.
     * @param selfTradePrevention self-trade prevention mode.
     * @param postOnly            post-only flag.
     * @return true if the command was handed to Aeron.
     */
    public boolean sendNewOrder(
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final Side side,
        final OrderType orderType,
        final TimeInForce timeInForce,
        final long price,
        final long quantity,
        final SelfTradePrevention selfTradePrevention,
        final boolean postOnly)
    {
        newOrder.wrapForEncode(sendBuffer, 0)
            .clientOrderId(clientOrderId)
            .userId(userId)
            .instrumentId(instrumentId)
            .side(side)
            .orderType(orderType)
            .timeInForce(timeInForce)
            .selfTradePrevention(selfTradePrevention)
            .flags(postOnly ? NewOrderFlyweight.FLAG_POST_ONLY : 0)
            .price(price)
            .quantity(quantity)
            .sendingTime(System.currentTimeMillis() * 1_000_000L);
        return offer(NewOrderFlyweight.ENCODED_LENGTH);
    }

    /**
     * Send a limit GTC order with no self-trade prevention (convenience).
     *
     * @param clientOrderId client order id.
     * @param userId        user.
     * @param instrumentId  instrument.
     * @param side          side.
     * @param price         limit price.
     * @param quantity      quantity.
     * @return true if sent.
     */
    public boolean sendLimitOrder(
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final Side side,
        final long price,
        final long quantity)
    {
        return sendNewOrder(clientOrderId, userId, instrumentId, side, OrderType.LIMIT, TimeInForce.GTC, price,
            quantity, SelfTradePrevention.NONE, false);
    }

    /**
     * Send a market order (convenience).
     *
     * @param clientOrderId client order id.
     * @param userId        user.
     * @param instrumentId  instrument.
     * @param side          side.
     * @param quantity      quantity.
     * @return true if sent.
     */
    public boolean sendMarketOrder(
        final long clientOrderId, final long userId, final int instrumentId, final Side side, final long quantity)
    {
        return sendNewOrder(clientOrderId, userId, instrumentId, side, OrderType.MARKET, TimeInForce.IOC, 0,
            quantity, SelfTradePrevention.NONE, false);
    }

    /**
     * Send a cancel request.
     *
     * @param clientOrderId client id of the cancel request.
     * @param userId        user.
     * @param instrumentId  instrument.
     * @param orderId       order to cancel.
     * @return true if sent.
     */
    public boolean sendCancel(final long clientOrderId, final long userId, final int instrumentId, final long orderId)
    {
        cancelOrder.wrapForEncode(sendBuffer, 0)
            .clientOrderId(clientOrderId)
            .userId(userId)
            .instrumentId(instrumentId)
            .orderId(orderId)
            .sendingTime(System.currentTimeMillis() * 1_000_000L);
        return offer(CancelOrderFlyweight.ENCODED_LENGTH);
    }

    /**
     * Request a snapshot on the market data stream.
     *
     * @param correlationId informational id.
     * @param instrumentId  instrument or {@link BookSnapshotRequestFlyweight#ALL_INSTRUMENTS}.
     * @return true if sent.
     */
    public boolean requestSnapshot(final long correlationId, final int instrumentId)
    {
        snapshotRequest.wrapForEncode(sendBuffer, 0).correlationId(correlationId).instrumentId(instrumentId);
        return offer(BookSnapshotRequestFlyweight.ENCODED_LENGTH);
    }

    /**
     * Poll execution events.
     *
     * @param listener      receiving decoded messages.
     * @param fragmentLimit maximum fragments.
     * @return fragments read.
     */
    public int pollExecutions(final EngineEventListener listener, final int fragmentLimit)
    {
        this.listener = listener;
        int fragments = 0;
        for (int i = 0; i < executionSubscriptions.length; i++)
        {
            fragments += executionSubscriptions[i].poll(executionAssemblers[i], fragmentLimit);
        }
        return fragments;
    }

    /**
     * Poll market data.
     *
     * @param listener      receiving decoded messages.
     * @param fragmentLimit maximum fragments.
     * @return fragments read.
     */
    public int pollMarketData(final EngineEventListener listener, final int fragmentLimit)
    {
        this.listener = listener;
        int fragments = 0;
        for (int i = 0; i < marketDataSubscriptions.length; i++)
        {
            fragments += marketDataSubscriptions[i].poll(marketDataAssemblers[i], fragmentLimit);
        }
        return fragments;
    }

    private boolean offer(final int length)
    {
        long deadlineNs = 0;
        while (true)
        {
            final long result = commandPublication.offer(sendBuffer, 0, length);
            if (result > 0)
            {
                return true;
            }
            if (result != Publication.BACK_PRESSURED && result != Publication.ADMIN_ACTION &&
                result != Publication.NOT_CONNECTED)
            {
                return false;
            }
            final long nowNs = System.nanoTime();
            if (deadlineNs == 0)
            {
                deadlineNs = nowNs + sendTimeoutNs;
            }
            else if (nowNs - deadlineNs >= 0)
            {
                return false;
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public void close()
    {
        CloseHelper.closeAll(commandPublication);
        CloseHelper.closeAll(executionSubscriptions);
        CloseHelper.closeAll(marketDataSubscriptions);
    }

    /**
     * Duplicate filter of the private event stream.
     *
     * @return deduplicator.
     */
    public OutputDeduplicator executionDeduplicator()
    {
        return executionDedup;
    }

    /**
     * Duplicate filter of the market data stream.
     *
     * @return deduplicator.
     */
    public OutputDeduplicator marketDataDeduplicator()
    {
        return marketDataDedup;
    }

    /**
     * Messages dropped because they came from an engine superseded by a newer epoch.
     *
     * @return count.
     */
    public long staleDropped()
    {
        return staleDropped;
    }
}
