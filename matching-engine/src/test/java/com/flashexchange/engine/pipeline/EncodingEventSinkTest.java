package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.matching.MatchingEngine;
import com.flashexchange.protocol.BookSnapshotFlyweight;
import com.flashexchange.protocol.BookUpdateFlyweight;
import com.flashexchange.protocol.EngineEventListener;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.PublicTradeFlyweight;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import com.flashexchange.protocol.TradeExecutionFlyweight;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EncodingEventSinkTest
{
    final EncodingEventSink sink = new EncodingEventSink();
    final MatchingEngine engine = new MatchingEngine(
        List.of(new Instrument(3, "EURUSD", 1, 1, 1, 1000, 0)), sink, 8, 0);
    final MessageBatch execution = new MessageBatch(16);
    final MessageBatch marketData = new MessageBatch(16);
    final CapturingPublisher executionOut = new CapturingPublisher();
    final CapturingPublisher marketDataOut = new CapturingPublisher();
    final List<String> decoded = new ArrayList<>();

    final EngineEventListener listener = new EngineEventListener()
    {
        @Override
        public void onExecutionReport(final ExecutionReportFlyweight r)
        {
            decoded.add("ER " + r.engineSequence() + " " + r.orderId() + " " + r.clientOrderId() + " " +
                r.userId() + " " + r.execType() + " " + r.orderStatus() + " " + r.side() + " " + r.orderType() +
                " " + r.timeInForce() + " px=" + r.price() + " qty=" + r.quantity() + " last=" + r.lastQuantity() +
                "@" + r.lastPrice() + " cum=" + r.cumulativeQuantity() + " leaves=" + r.leavesQuantity() +
                " trade=" + (r.tradeId() & 0xFFFFFFFFFFL) + " " + r.liquidity() + " " + r.reason());
        }

        @Override
        public void onTradeExecution(final TradeExecutionFlyweight t)
        {
            decoded.add("TE " + (t.tradeId() & 0xFFFFFFFFFFL) + " " + t.quantity() + "@" + t.price() + " buy=" +
                t.buyOrderId() + "/" + t.buyUserId() + "/" + t.buyClientOrderId() + " sell=" + t.sellOrderId() + "/" +
                t.sellUserId() + "/" + t.sellClientOrderId() + " " + t.aggressorSide() + " i=" + t.instrumentId());
        }

        @Override
        public void onPublicTrade(final PublicTradeFlyweight t)
        {
            decoded.add("PT " + t.quantity() + "@" + t.price() + " " + t.aggressorSide() + " i=" + t.instrumentId());
        }

        @Override
        public void onBookUpdate(final BookUpdateFlyweight u)
        {
            final StringBuilder sb = new StringBuilder("BU seq=" + u.bookSequence() + " i=" + u.instrumentId());
            for (int i = 0; i < u.levelCount(); i++)
            {
                sb.append(' ').append(u.levelSide(i)).append(':').append(u.levelPrice(i)).append('=')
                    .append(u.levelQuantity(i)).append('/').append(u.levelOrderCount(i));
            }
            decoded.add(sb.toString());
        }

        @Override
        public void onBookSnapshot(final BookSnapshotFlyweight s)
        {
            final StringBuilder sb = new StringBuilder("BS seq=" + s.bookSequence());
            for (int i = 0; i < s.bidCount(); i++)
            {
                sb.append(" B").append(s.bidPrice(i)).append('=').append(s.bidQuantity(i)).append('/')
                    .append(s.bidOrderCount(i));
            }
            for (int i = 0; i < s.askCount(); i++)
            {
                sb.append(" A").append(s.askPrice(i)).append('=').append(s.askQuantity(i)).append('/')
                    .append(s.askOrderCount(i));
            }
            decoded.add(sb.toString());
        }
    };

    void flush()
    {
        execution.drainTo(executionOut);
        marketData.drainTo(marketDataOut);
        execution.clear();
        marketData.clear();
    }

    @Test
    void encodesTheCompleteEventStream()
    {
        sink.bind(execution, marketData);
        engine.onNewOrder(0, 10, 1, 501, 7, 3, Side.SELL.code(), OrderType.LIMIT.code(), TimeInForce.GTC.code(),
            SelfTradePrevention.NONE.code(), false, 100, 5);
        engine.onNewOrder(1, 20, 2, 502, 8, 3, Side.BUY.code(), OrderType.LIMIT.code(), TimeInForce.GTC.code(),
            SelfTradePrevention.NONE.code(), false, 101, 8);
        engine.onNewOrder(2, 30, 3, 503, 8, 3, (byte)9, OrderType.LIMIT.code(), TimeInForce.GTC.code(),
            SelfTradePrevention.NONE.code(), false, 101, 8);
        engine.onCancelOrder(3, 40, 504, 9, 3, 2);
        engine.onCancelOrder(4, 50, 505, 8, 3, 2);
        engine.onBookSnapshotRequest(5, 60, 3);
        flush();

        executionOut.replay(listener);
        marketDataOut.replay(listener);
        assertEquals(List.of(
            "ER 0 1 501 7 NEW NEW SELL LIMIT GTC px=100 qty=5 last=0@0 cum=0 leaves=5 trade=0 NONE NONE",
            "ER 1 2 502 8 NEW NEW BUY LIMIT GTC px=101 qty=8 last=0@0 cum=0 leaves=8 trade=0 NONE NONE",
            "TE 1 5@100 buy=2/8/502 sell=1/7/501 BUY i=3",
            "ER 1 1 501 7 TRADE FILLED SELL LIMIT GTC px=100 qty=5 last=5@100 cum=5 leaves=0 trade=1 MAKER NONE",
            "ER 1 2 502 8 TRADE PARTIALLY_FILLED BUY LIMIT GTC px=101 qty=8 last=5@100 cum=5 leaves=3 trade=1 " +
                "TAKER NONE",
            "ER 2 3 503 8 REJECTED REJECTED null LIMIT GTC px=101 qty=8 last=0@0 cum=0 leaves=0 trade=0 NONE " +
                "INVALID_SIDE",
            "ER 3 2 504 9 CANCEL_REJECTED REJECTED null null null px=0 qty=0 last=0@0 cum=0 leaves=0 trade=0 NONE " +
                "NOT_ORDER_OWNER",
            "ER 4 2 502 8 CANCELED CANCELED BUY LIMIT GTC px=101 qty=8 last=0@0 cum=5 leaves=0 trade=0 NONE " +
                "USER_CANCEL",
            "BU seq=1 i=3 SELL:100=5/1",
            "PT 5@100 BUY i=3",
            "BU seq=2 i=3 SELL:100=0/0 BUY:101=3/1",
            "BU seq=3 i=3 BUY:101=0/0",
            "BS seq=3"), decoded);
        assertEquals(Liquidity.NONE.code(), 0);
        assertEquals(ExecType.NEW.code(), 1);
        assertEquals(OrderStatus.NEW.code(), 1);
        assertEquals(Reason.NONE.code(), 0);
    }

    @Test
    void snapshotHonoursDepth()
    {
        final EncodingEventSink depthSink = new EncodingEventSink();
        final MatchingEngine shallow = new MatchingEngine(
            List.of(new Instrument(3, "EURUSD", 1, 1, 1, 1000, 0)), depthSink, 8, 1);
        depthSink.bind(execution, marketData);
        for (int i = 0; i < 3; i++)
        {
            shallow.onNewOrder(i, 0, i + 1, i, 1, 3, Side.BUY.code(), OrderType.LIMIT.code(), TimeInForce.GTC.code(),
                (byte)0, false, 90 + i, 1);
            shallow.onNewOrder(i, 0, i + 11, i, 1, 3, Side.SELL.code(), OrderType.LIMIT.code(),
                TimeInForce.GTC.code(), (byte)0, false, 110 + i, 2);
        }
        execution.clear();
        marketData.clear();
        shallow.onBookSnapshotRequest(9, 0, 3);
        flush();
        marketDataOut.replay(listener);
        assertEquals(List.of("BS seq=6 B92=1/1 A110=2/1"), decoded);
    }
}
