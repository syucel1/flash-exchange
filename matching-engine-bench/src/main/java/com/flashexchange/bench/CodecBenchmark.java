package com.flashexchange.bench;

import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.NewOrderFlyweight;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Encode/decode cost of the binary protocol flyweights.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED"})
@State(Scope.Thread)
public class CodecBenchmark
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
    private final NewOrderFlyweight newOrder = new NewOrderFlyweight();
    private final ExecutionReportFlyweight report = new ExecutionReportFlyweight();
    private long counter;

    /**
     * Encode a new order command.
     *
     * @return encoded length.
     */
    @Benchmark
    public int encodeNewOrder()
    {
        return newOrder.wrapForEncode(buffer, 0)
            .clientOrderId(++counter).userId(7).instrumentId(1).side(Side.BUY).orderType(OrderType.LIMIT)
            .timeInForce(TimeInForce.GTC).selfTradePrevention(SelfTradePrevention.NONE).flags((byte)0)
            .price(108_345).quantity(100_000).sendingTime(counter).encodedLength();
    }

    /**
     * Encode then decode an execution report.
     *
     * @return a decoded field.
     */
    @Benchmark
    public long encodeDecodeExecutionReport()
    {
        report.wrapForEncode(buffer, 0)
            .engineSequence(++counter).timestamp(counter).orderId(counter).clientOrderId(1).userId(2).price(3)
            .quantity(4).lastPrice(5).lastQuantity(6).cumulativeQuantity(7).leavesQuantity(8).tradeId(9)
            .instrumentId(1).execType(ExecType.TRADE).orderStatus(OrderStatus.PARTIALLY_FILLED).side(Side.SELL)
            .orderType(OrderType.LIMIT).timeInForce(TimeInForce.GTC).liquidity(Liquidity.MAKER).reason(Reason.NONE);
        report.wrapForDecode(buffer, 0);
        return report.orderId() + report.cumulativeQuantity() + report.execTypeCode();
    }
}
