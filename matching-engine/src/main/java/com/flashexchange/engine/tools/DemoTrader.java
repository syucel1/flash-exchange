package com.flashexchange.engine.tools;

import com.flashexchange.engine.client.EngineClient;
import com.flashexchange.engine.config.TransportMode;
import com.flashexchange.engine.tools.DemoTraderConfig.DemoInstrument;
import com.flashexchange.protocol.BookSnapshotFlyweight;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.BookUpdateFlyweight;
import com.flashexchange.protocol.EngineEventListener;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.PublicTradeFlyweight;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Demo client and load generator: a population of simulated users sending random limit orders around a moving
 * reference price, some market orders and cancels of their own resting orders, while consuming execution events
 * and market data and printing a periodic summary. Used by the docker-compose stack and for smoke testing a
 * deployed engine.
 * <pre>
 *   java -cp matching-engine-all.jar com.flashexchange.engine.tools.DemoTrader [demo.properties]
 * </pre>
 */
public final class DemoTrader implements AutoCloseable, EngineEventListener
{
    private final DemoTraderConfig config;
    private final PrintStream out;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final EngineClient client;
    private final Random random;
    private final Int2ObjectHashMap<InstrumentState> instruments = new Int2ObjectHashMap<>();
    private final LongArrayList liveOrderIds = new LongArrayList();
    private final LongArrayList liveOrderUsers = new LongArrayList();
    private final LongArrayList liveOrderInstruments = new LongArrayList();
    private final Stats stats = new Stats();
    private long nextClientOrderId = 1;

    /** Counters of what the simulator sent and observed. */
    public static final class Stats
    {
        long ordersSent;
        long cancelsSent;
        long sendFailures;
        long accepted;
        long rejected;
        long cancelRejected;
        long fills;
        long cancelled;
        long publicTrades;
        long tradedQuantity;
        long bookUpdates;
        long snapshots;

        /**
         * Orders sent.
         *
         * @return count.
         */
        public long ordersSent()
        {
            return ordersSent;
        }

        /**
         * Cancels sent.
         *
         * @return count.
         */
        public long cancelsSent()
        {
            return cancelsSent;
        }

        /**
         * Commands that could not be sent.
         *
         * @return count.
         */
        public long sendFailures()
        {
            return sendFailures;
        }

        /**
         * NEW acknowledgements for the simulator's orders.
         *
         * @return count.
         */
        public long accepted()
        {
            return accepted;
        }

        /**
         * Rejected orders of the simulator.
         *
         * @return count.
         */
        public long rejected()
        {
            return rejected;
        }

        /**
         * Rejected cancels of the simulator (e.g. order filled in between).
         *
         * @return count.
         */
        public long cancelRejected()
        {
            return cancelRejected;
        }

        /**
         * Fill reports for the simulator's orders (maker and taker sides).
         *
         * @return count.
         */
        public long fills()
        {
            return fills;
        }

        /**
         * Cancel reports for the simulator's orders.
         *
         * @return count.
         */
        public long cancelled()
        {
            return cancelled;
        }

        /**
         * Public trades seen on the market data stream.
         *
         * @return count.
         */
        public long publicTrades()
        {
            return publicTrades;
        }

        /**
         * Quantity traded according to public trades.
         *
         * @return quantity.
         */
        public long tradedQuantity()
        {
            return tradedQuantity;
        }

        /**
         * Book updates received.
         *
         * @return count.
         */
        public long bookUpdates()
        {
            return bookUpdates;
        }

        /**
         * Book snapshots received.
         *
         * @return count.
         */
        public long snapshots()
        {
            return snapshots;
        }
    }

    private static final class InstrumentState
    {
        final DemoInstrument instrument;
        final TreeMap<Long, Long> bids = new TreeMap<>();
        final TreeMap<Long, Long> asks = new TreeMap<>();
        long referencePrice;
        long lastTradePrice;

        InstrumentState(final DemoInstrument instrument)
        {
            this.instrument = instrument;
            this.referencePrice = instrument.basePrice();
        }
    }

    /**
     * Connect according to the configuration.
     *
     * @param config configuration.
     * @param out    where reports are printed.
     */
    public DemoTrader(final DemoTraderConfig config, final PrintStream out)
    {
        this.config = config;
        this.out = out;
        this.random = new Random(config.seed());
        for (final DemoInstrument instrument : config.instruments())
        {
            instruments.put(instrument.id(), new InstrumentState(instrument));
        }

        MediaDriver driver = null;
        Aeron connected = null;
        try
        {
            String directory = config.aeronDirectory();
            if (config.embeddedMediaDriver())
            {
                final MediaDriver.Context ctx = new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true);
                if (directory != null)
                {
                    ctx.aeronDirectoryName(directory);
                }
                driver = MediaDriver.launch(ctx);
                directory = driver.aeronDirectoryName();
            }
            final Aeron.Context ctx = new Aeron.Context();
            if (directory != null)
            {
                ctx.aeronDirectoryName(directory);
            }
            connected = Aeron.connect(ctx);
            this.client = new EngineClient(connected, config.channels(), 1_000);
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.quietCloseAll(connected, driver);
            throw ex;
        }
        this.mediaDriver = driver;
        this.aeron = connected;
    }

    /**
     * Wait for the engine streams to connect.
     *
     * @param timeoutMs maximum wait.
     * @return true when connected.
     */
    public boolean awaitConnected(final long timeoutMs)
    {
        return client.awaitConnected(timeoutMs);
    }

    /**
     * Statistics so far.
     *
     * @return stats.
     */
    public Stats stats()
    {
        return stats;
    }

    /**
     * Generate load until the configured limits are reached or {@code keepRunning} returns false. Requests a
     * snapshot of every instrument first so the local books are complete.
     *
     * @param keepRunning external stop condition.
     */
    public void run(final BooleanSupplier keepRunning)
    {
        client.requestSnapshot(0, BookSnapshotRequestFlyweight.ALL_INSTRUMENTS);
        final long intervalNs = TimeUnit.SECONDS.toNanos(1) / config.ordersPerSecond();
        final long startNs = System.nanoTime();
        final long endNs = config.durationSeconds() > 0 ?
            startNs + TimeUnit.SECONDS.toNanos(config.durationSeconds()) : Long.MAX_VALUE;
        final long reportIntervalNs = TimeUnit.SECONDS.toNanos(config.reportIntervalSeconds());
        long nextSendNs = startNs;
        long nextReportNs = reportIntervalNs > 0 ? startNs + reportIntervalNs : Long.MAX_VALUE;

        while (keepRunning.getAsBoolean())
        {
            final long nowNs = System.nanoTime();
            if (nowNs - endNs >= 0 || (config.maxOrders() > 0 && stats.ordersSent + stats.cancelsSent >=
                config.maxOrders()))
            {
                break;
            }
            if (nowNs - nextSendNs >= 0)
            {
                sendRandomCommand();
                nextSendNs += intervalNs;
            }
            if (nowNs - nextReportNs >= 0)
            {
                report();
                nextReportNs += reportIntervalNs;
            }
            if (poll() == 0)
            {
                Thread.onSpinWait();
            }
        }

        final long drainUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        while (System.nanoTime() - drainUntil < 0)
        {
            poll();
        }
        report();
    }

    /**
     * Poll both engine streams once.
     *
     * @return fragments read.
     */
    public int poll()
    {
        return client.pollExecutions(this, 256) + client.pollMarketData(this, 256);
    }

    private void sendRandomCommand()
    {
        final int roll = random.nextInt(100);
        if (roll < config.cancelPercent() && !liveOrderIds.isEmpty())
        {
            final int index = random.nextInt(liveOrderIds.size());
            final long orderId = liveOrderIds.getLong(index);
            final long userId = liveOrderUsers.getLong(index);
            final int instrumentId = (int)liveOrderInstruments.getLong(index);
            removeLive(index);
            if (client.sendCancel(nextClientOrderId++, userId, instrumentId, orderId))
            {
                stats.cancelsSent++;
            }
            else
            {
                stats.sendFailures++;
            }
            return;
        }

        final DemoInstrument instrument = config.instruments().get(random.nextInt(config.instruments().size()));
        final InstrumentState state = instruments.get(instrument.id());
        final long userId = config.userBase() + random.nextInt(config.users());
        final Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
        final long quantity = (1 + random.nextInt(config.maxLots())) * instrument.lotSize();
        final boolean market = roll < config.cancelPercent() + config.marketOrderPercent();

        final boolean sent;
        if (market)
        {
            sent = client.sendNewOrder(nextClientOrderId++, userId, instrument.id(), side, OrderType.MARKET,
                TimeInForce.IOC, 0, quantity, SelfTradePrevention.CANCEL_TAKER, false);
        }
        else
        {
            // passive orders mostly away from the reference, aggressive ones crossing it
            final int offset = random.nextInt(config.spreadTicks()) - config.spreadTicks() / 4;
            final long price = Math.max(instrument.tickSize(),
                state.referencePrice + (side == Side.BUY ? -offset : offset) * instrument.tickSize());
            sent = client.sendNewOrder(nextClientOrderId++, userId, instrument.id(), side, OrderType.LIMIT,
                TimeInForce.GTC, price, quantity, SelfTradePrevention.CANCEL_TAKER, false);
        }
        if (sent)
        {
            stats.ordersSent++;
        }
        else
        {
            stats.sendFailures++;
        }
    }

    private boolean isOwnUser(final long userId)
    {
        return userId >= config.userBase() && userId < config.userBase() + config.users();
    }

    private void removeLive(final int index)
    {
        final int last = liveOrderIds.size() - 1;
        liveOrderIds.setLong(index, liveOrderIds.getLong(last));
        liveOrderUsers.setLong(index, liveOrderUsers.getLong(last));
        liveOrderInstruments.setLong(index, liveOrderInstruments.getLong(last));
        liveOrderIds.removeAt(last);
        liveOrderUsers.removeAt(last);
        liveOrderInstruments.removeAt(last);
    }

    private void removeLiveOrder(final long orderId)
    {
        for (int i = liveOrderIds.size() - 1; i >= 0; i--)
        {
            if (liveOrderIds.getLong(i) == orderId)
            {
                removeLive(i);
                return;
            }
        }
    }

    @Override
    public void onExecutionReport(final ExecutionReportFlyweight report)
    {
        if (!isOwnUser(report.userId()))
        {
            return;
        }
        final ExecType execType = report.execType();
        if (execType == ExecType.NEW)
        {
            stats.accepted++;
            if (report.orderType() == OrderType.LIMIT)
            {
                liveOrderIds.addLong(report.orderId());
                liveOrderUsers.addLong(report.userId());
                liveOrderInstruments.addLong(report.instrumentId());
            }
        }
        else if (execType == ExecType.TRADE)
        {
            stats.fills++;
            if (report.orderStatus() == OrderStatus.FILLED)
            {
                removeLiveOrder(report.orderId());
            }
        }
        else if (execType == ExecType.CANCELED)
        {
            stats.cancelled++;
            removeLiveOrder(report.orderId());
        }
        else if (execType == ExecType.REJECTED)
        {
            stats.rejected++;
        }
        else
        {
            stats.cancelRejected++;
        }
    }

    @Override
    public void onPublicTrade(final PublicTradeFlyweight trade)
    {
        stats.publicTrades++;
        stats.tradedQuantity += trade.quantity();
        final InstrumentState state = instruments.get(trade.instrumentId());
        if (state != null)
        {
            state.lastTradePrice = trade.price();
            state.referencePrice = trade.price();
        }
    }

    @Override
    public void onBookUpdate(final BookUpdateFlyweight update)
    {
        stats.bookUpdates++;
        final InstrumentState state = instruments.get(update.instrumentId());
        if (state == null)
        {
            return;
        }
        for (int i = 0; i < update.levelCount(); i++)
        {
            final TreeMap<Long, Long> side = update.levelSide(i) == Side.BUY ? state.bids : state.asks;
            if (update.levelQuantity(i) == 0)
            {
                side.remove(update.levelPrice(i));
            }
            else
            {
                side.put(update.levelPrice(i), update.levelQuantity(i));
            }
        }
    }

    @Override
    public void onBookSnapshot(final BookSnapshotFlyweight snapshot)
    {
        stats.snapshots++;
        final InstrumentState state = instruments.get(snapshot.instrumentId());
        if (state == null)
        {
            return;
        }
        state.bids.clear();
        state.asks.clear();
        for (int i = 0; i < snapshot.bidCount(); i++)
        {
            state.bids.put(snapshot.bidPrice(i), snapshot.bidQuantity(i));
        }
        for (int i = 0; i < snapshot.askCount(); i++)
        {
            state.asks.put(snapshot.askPrice(i), snapshot.askQuantity(i));
        }
    }

    /**
     * Print a summary line per instrument.
     */
    public void report()
    {
        out.printf("orders=%d cancels=%d accepted=%d rejected=%d fills=%d cancelled=%d trades=%d volume=%d " +
            "updates=%d snapshots=%d sendFailures=%d%n", stats.ordersSent, stats.cancelsSent, stats.accepted,
            stats.rejected, stats.fills, stats.cancelled, stats.publicTrades, stats.tradedQuantity,
            stats.bookUpdates, stats.snapshots, stats.sendFailures);
        for (final DemoInstrument instrument : config.instruments())
        {
            final InstrumentState state = instruments.get(instrument.id());
            final Map.Entry<Long, Long> bid = state.bids.lastEntry();
            final Map.Entry<Long, Long> ask = state.asks.firstEntry();
            out.printf("  %-8s last=%d bid=%s ask=%s levels=%d/%d%n", instrument.symbol(), state.lastTradePrice,
                bid == null ? "-" : bid.getValue() + "@" + bid.getKey(),
                ask == null ? "-" : ask.getValue() + "@" + ask.getKey(), state.bids.size(), state.asks.size());
        }
        out.flush();
    }

    /**
     * Best bid of an instrument according to the locally maintained book.
     *
     * @param instrumentId instrument.
     * @return best bid price or 0.
     */
    public long bestBid(final int instrumentId)
    {
        final Map.Entry<Long, Long> e = instruments.get(instrumentId).bids.lastEntry();
        return e == null ? 0 : e.getKey();
    }

    /**
     * Best ask of an instrument according to the locally maintained book.
     *
     * @param instrumentId instrument.
     * @return best ask price or 0.
     */
    public long bestAsk(final int instrumentId)
    {
        final Map.Entry<Long, Long> e = instruments.get(instrumentId).asks.firstEntry();
        return e == null ? 0 : e.getKey();
    }

    /**
     * Number of the simulator's orders believed to be resting.
     *
     * @return count.
     */
    public int liveOrderCount()
    {
        return liveOrderIds.size();
    }

    @Override
    public void close()
    {
        CloseHelper.closeAll(client, aeron, mediaDriver);
    }

    /**
     * Run with an optional properties file until the configured limits or SIGINT/SIGTERM.
     *
     * @param args optional path of a properties file.
     * @throws Exception on configuration errors.
     */
    public static void main(final String[] args) throws Exception
    {
        final DemoTraderConfig config = DemoTraderConfig.load(args.length > 0 ? Path.of(args[0]) : null);
        final int code = run(config, System.out, new ShutdownSignalBarrier());
        if (code != 0)
        {
            System.exit(code);
        }
    }

    /**
     * Run until finished or the barrier is signalled.
     *
     * @param config  configuration.
     * @param out     report destination.
     * @param barrier shutdown signal.
     * @return process exit code (0 = ok, 2 = could not connect).
     */
    public static int run(final DemoTraderConfig config, final PrintStream out, final ShutdownSignalBarrier barrier)
    {
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread waiter = new Thread(() ->
        {
            barrier.await();
            running.set(false);
        }, "demo-shutdown");
        waiter.setDaemon(true);
        waiter.start();

        out.println("demo trader connecting: transport=" + config.transport() +
            (config.transport() == TransportMode.UDP ? " engine=" + config.engineHost() : "") +
            " instruments=" + config.instruments().size() + " users=" + config.users() +
            " rate=" + config.ordersPerSecond() + "/s");
        try (DemoTrader trader = new DemoTrader(config, out))
        {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (!trader.awaitConnected(1_000))
            {
                if (!running.get() || System.nanoTime() > deadline)
                {
                    out.println("demo trader: engine not reachable");
                    return 2;
                }
            }
            out.println("demo trader connected");
            trader.run(running::get);
        }
        return 0;
    }
}
