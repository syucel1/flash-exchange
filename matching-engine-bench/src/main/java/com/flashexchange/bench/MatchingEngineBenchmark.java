package com.flashexchange.bench;

import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.matching.MatchingEngine;
import com.flashexchange.engine.pipeline.EncodingEventSink;
import com.flashexchange.engine.pipeline.MessageBatch;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Single-threaded matching engine cost per command, including encoding of every output message (no transport).
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED", "-Xms2g", "-Xmx2g"})
@State(Scope.Thread)
public class MatchingEngineBenchmark
{
    private static final int INSTRUMENT = 1;
    private static final byte LIMIT = OrderType.LIMIT.code();
    private static final byte MARKET = OrderType.MARKET.code();
    private static final byte GTC = TimeInForce.GTC.code();
    private static final byte IOC = TimeInForce.IOC.code();
    private static final byte NO_STP = SelfTradePrevention.NONE.code();
    private static final int FLOW_SIZE = 1 << 16;

    private MatchingEngine engine;
    private MessageBatch executionOutput;
    private MessageBatch marketDataOutput;
    private long sequence;

    private byte[] flowSide;
    private byte[] flowType;
    private long[] flowPrice;
    private long[] flowQuantity;
    private int flowIndex;

    /**
     * Build an engine with a two-sided book 50 levels deep.
     */
    @Setup(Level.Trial)
    public void setUp()
    {
        final EncodingEventSink sink = new EncodingEventSink();
        engine = new MatchingEngine(List.of(new Instrument(INSTRUMENT, "EURUSD", 1, 1, 1, 1_000_000_000L, 500)),
            sink, 1 << 20, 0);
        executionOutput = new MessageBatch(1 << 16);
        marketDataOutput = new MessageBatch(1 << 16);
        sink.bind(executionOutput, marketDataOutput);

        for (int level = 1; level <= 50; level++)
        {
            for (int i = 0; i < 4; i++)
            {
                submit(Side.BUY.code(), LIMIT, GTC, 100_000 - level, 1_000);
                submit(Side.SELL.code(), LIMIT, GTC, 100_000 + level, 1_000);
            }
        }

        final Random random = new Random(42);
        flowSide = new byte[FLOW_SIZE];
        flowType = new byte[FLOW_SIZE];
        flowPrice = new long[FLOW_SIZE];
        flowQuantity = new long[FLOW_SIZE];
        for (int i = 0; i < FLOW_SIZE; i++)
        {
            flowSide[i] = random.nextBoolean() ? Side.BUY.code() : Side.SELL.code();
            flowType[i] = random.nextInt(10) == 0 ? MARKET : LIMIT;
            final int offset = random.nextInt(60) - 10;
            flowPrice[i] = flowType[i] == MARKET ? 0 :
                flowSide[i] == Side.BUY.code() ? 100_000 - offset : 100_000 + offset;
            flowQuantity[i] = 100 + random.nextInt(2_000);
        }
    }

    private long submit(final byte side, final byte type, final byte tif, final long price, final long quantity)
    {
        final long seq = sequence++;
        engine.onNewOrder(seq, seq, seq + 1, seq, 1 + (seq & 63), INSTRUMENT, side, type, tif, NO_STP, false,
            price, quantity);
        executionOutput.clear();
        marketDataOutput.clear();
        return seq + 1;
    }

    /**
     * Add a passive order behind the book and cancel it: the cost of the order lifecycle without a trade.
     *
     * @return order id (consumed by JMH).
     */
    @Benchmark
    public long placeAndCancelPassiveOrder()
    {
        final long orderId = submit(Side.BUY.code(), LIMIT, GTC, 99_000, 1_000);
        final long seq = sequence++;
        engine.onCancelOrder(seq, seq, seq, 1 + ((orderId - 1) & 63), INSTRUMENT, orderId);
        executionOutput.clear();
        marketDataOutput.clear();
        return orderId;
    }

    /**
     * An IOC order that takes one resting order, followed by a replenishing passive order.
     *
     * @return order id (consumed by JMH).
     */
    @Benchmark
    public long aggressiveFillAndReplenish()
    {
        submit(Side.BUY.code(), LIMIT, IOC, 100_001, 1_000);
        return submit(Side.SELL.code(), LIMIT, GTC, 100_001, 1_000);
    }

    /**
     * Pre-generated random flow: 90 % limit orders around the touch, 10 % market orders. The book grows over the
     * run like a real one does.
     *
     * @return order id (consumed by JMH).
     */
    @Benchmark
    public long randomFlow()
    {
        final int i = flowIndex++ & (FLOW_SIZE - 1);
        return submit(flowSide[i], flowType[i], flowType[i] == MARKET ? IOC : GTC, flowPrice[i], flowQuantity[i]);
    }
}
