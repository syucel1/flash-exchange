package com.flashexchange.engine.client;

import com.flashexchange.protocol.BookSnapshotFlyweight;
import com.flashexchange.protocol.BookUpdateFlyweight;
import com.flashexchange.protocol.EngineEventListener;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.PublicTradeFlyweight;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TradeExecutionFlyweight;

import java.util.ArrayList;
import java.util.List;

/**
 * Copies decoded engine output into records for assertions in integration tests.
 */
public final class RecordingListener implements EngineEventListener
{
    public record Report(long orderId, long clientOrderId, long userId, ExecType execType, OrderStatus status,
        long lastPrice, long lastQuantity, long cumQuantity, long leavesQuantity, Reason reason)
    {
    }

    public record TradeRecord(long tradeId, long price, long quantity, long buyOrderId, long sellOrderId,
        Side aggressor)
    {
    }

    public record Update(int instrumentId, long bookSequence, List<long[]> levels)
    {
    }

    public record SnapshotRecord(int instrumentId, long bookSequence, List<long[]> bids, List<long[]> asks)
    {
    }

    public final List<Report> reports = new ArrayList<>();
    public final List<TradeRecord> trades = new ArrayList<>();
    public final List<TradeRecord> publicTrades = new ArrayList<>();
    public final List<Update> updates = new ArrayList<>();
    public final List<SnapshotRecord> snapshots = new ArrayList<>();
    public int unknown;

    @Override
    public void onExecutionReport(final ExecutionReportFlyweight r)
    {
        reports.add(new Report(r.orderId(), r.clientOrderId(), r.userId(), r.execType(), r.orderStatus(),
            r.lastPrice(), r.lastQuantity(), r.cumulativeQuantity(), r.leavesQuantity(), r.reason()));
    }

    @Override
    public void onTradeExecution(final TradeExecutionFlyweight t)
    {
        trades.add(new TradeRecord(t.tradeId(), t.price(), t.quantity(), t.buyOrderId(), t.sellOrderId(),
            t.aggressorSide()));
    }

    @Override
    public void onPublicTrade(final PublicTradeFlyweight t)
    {
        publicTrades.add(new TradeRecord(t.tradeId(), t.price(), t.quantity(), 0, 0, t.aggressorSide()));
    }

    @Override
    public void onBookUpdate(final BookUpdateFlyweight u)
    {
        final List<long[]> levels = new ArrayList<>();
        for (int i = 0; i < u.levelCount(); i++)
        {
            levels.add(new long[] {u.levelSide(i).code(), u.levelPrice(i), u.levelQuantity(i), u.levelOrderCount(i)});
        }
        updates.add(new Update(u.instrumentId(), u.bookSequence(), levels));
    }

    @Override
    public void onBookSnapshot(final BookSnapshotFlyweight s)
    {
        final List<long[]> bids = new ArrayList<>();
        final List<long[]> asks = new ArrayList<>();
        for (int i = 0; i < s.bidCount(); i++)
        {
            bids.add(new long[] {s.bidPrice(i), s.bidQuantity(i), s.bidOrderCount(i)});
        }
        for (int i = 0; i < s.askCount(); i++)
        {
            asks.add(new long[] {s.askPrice(i), s.askQuantity(i), s.askOrderCount(i)});
        }
        snapshots.add(new SnapshotRecord(s.instrumentId(), s.bookSequence(), bids, asks));
    }

    @Override
    public void onUnknownMessage(final int templateId)
    {
        unknown++;
    }

    public List<Report> reportsFor(final long orderId)
    {
        return reports.stream().filter(r -> r.orderId() == orderId).toList();
    }
}
