package com.flashexchange.engine.client;

import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.CancelOrderFlyweight;
import com.flashexchange.protocol.EngineEventListener;
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
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

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
    private final Publication commandPublication;
    private final Subscription executionSubscription;
    private final Subscription marketDataSubscription;
    private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(256);
    private final NewOrderFlyweight newOrder = new NewOrderFlyweight();
    private final CancelOrderFlyweight cancelOrder = new CancelOrderFlyweight();
    private final BookSnapshotRequestFlyweight snapshotRequest = new BookSnapshotRequestFlyweight();
    private final EngineMessageDispatcher dispatcher = new EngineMessageDispatcher();
    private final FragmentAssembler executionAssembler;
    private final FragmentAssembler marketDataAssembler;
    private final long sendTimeoutNs;
    private EngineEventListener listener;

    /**
     * Create publication and subscriptions using client-side channels.
     *
     * @param aeron         connected Aeron client.
     * @param channels      client-side channels (see {@link EngineChannels#udpClientSide}) or IPC.
     * @param sendTimeoutMs how long a send retries on back-pressure before giving up.
     */
    public EngineClient(final Aeron aeron, final EngineChannels channels, final long sendTimeoutMs)
    {
        this.commandPublication = aeron.addPublication(channels.commandChannel(), channels.commandStreamId());
        this.executionSubscription =
            aeron.addSubscription(channels.executionChannel(), channels.executionStreamId());
        this.marketDataSubscription =
            aeron.addSubscription(channels.marketDataChannel(), channels.marketDataStreamId());
        this.sendTimeoutNs = TimeUnit.MILLISECONDS.toNanos(sendTimeoutMs);
        this.executionAssembler = new FragmentAssembler(
            (buffer, offset, length, header) -> dispatcher.dispatch(buffer, offset, length, listener));
        this.marketDataAssembler = new FragmentAssembler(
            (buffer, offset, length, header) -> dispatcher.dispatch(buffer, offset, length, listener));
    }

    /**
     * Whether the command publication and both subscriptions are connected to the engine.
     *
     * @return true when ready.
     */
    public boolean isConnected()
    {
        return commandPublication.isConnected() &&
            executionSubscription.isConnected() &&
            marketDataSubscription.isConnected();
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
        return executionSubscription.poll(executionAssembler, fragmentLimit);
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
        return marketDataSubscription.poll(marketDataAssembler, fragmentLimit);
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
        CloseHelper.closeAll(commandPublication, executionSubscription, marketDataSubscription);
    }
}
