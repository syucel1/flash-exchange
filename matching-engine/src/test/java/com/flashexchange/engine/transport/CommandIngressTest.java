package com.flashexchange.engine.transport;

import com.flashexchange.engine.pipeline.CommandType;
import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.CancelOrderFlyweight;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.NewOrderFlyweight;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import com.lmax.disruptor.RingBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandIngressTest
{
    final RingBuffer<OrderCommand> ring = RingBuffer.createSingleProducer(OrderCommand::new, 64);
    final long[] now = {1_000};
    final EpochNanoClock clock = () -> now[0];
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);

    @Test
    void decodesNewOrderIntoRingSlot()
    {
        final CommandIngress ingress = new CommandIngress(null, ring, clock, 10, new int[] {1, 2}, 0);
        new NewOrderFlyweight().wrapForEncode(buffer, 4).clientOrderId(7).userId(8).instrumentId(1)
            .side(Side.SELL).orderType(OrderType.LIMIT).timeInForce(TimeInForce.IOC)
            .selfTradePrevention(SelfTradePrevention.CANCEL_MAKER).flags(NewOrderFlyweight.FLAG_POST_ONLY)
            .price(100).quantity(5);
        ingress.onMessage(buffer, 4, NewOrderFlyweight.ENCODED_LENGTH);

        assertEquals(0, ring.getCursor());
        final OrderCommand command = ring.get(0);
        assertEquals(CommandType.NEW_ORDER, command.type());
        assertEquals(0, command.engineSequence());
        assertEquals(1_000, command.timestamp());
        assertEquals(1, command.orderId());
        assertEquals(1, command.instrumentId());
        assertEquals(1, ingress.commandsReceived());
        assertEquals("engine-ingress", ingress.roleName());
    }

    @Test
    void decodesCancelAndSnapshotRequests()
    {
        final CommandIngress ingress = new CommandIngress(null, ring, clock, 10, new int[] {1, 2, 3}, 0);
        new CancelOrderFlyweight().wrapForEncode(buffer, 0).clientOrderId(1).userId(2).instrumentId(3).orderId(44);
        ingress.onMessage(buffer, 0, CancelOrderFlyweight.ENCODED_LENGTH);
        assertEquals(CommandType.CANCEL_ORDER, ring.get(0).type());
        assertEquals(44, ring.get(0).orderId());

        new BookSnapshotRequestFlyweight().wrapForEncode(buffer, 0).instrumentId(2);
        ingress.onMessage(buffer, 0, BookSnapshotRequestFlyweight.ENCODED_LENGTH);
        assertEquals(CommandType.BOOK_SNAPSHOT, ring.get(1).type());
        assertEquals(2, ring.get(1).instrumentId());

        new BookSnapshotRequestFlyweight().wrapForEncode(buffer, 0)
            .instrumentId(BookSnapshotRequestFlyweight.ALL_INSTRUMENTS);
        ingress.onMessage(buffer, 0, BookSnapshotRequestFlyweight.ENCODED_LENGTH);
        assertEquals(4, ring.getCursor());
        assertEquals(3, ring.get(4).instrumentId());
        assertEquals(3, ingress.commandsReceived());
        assertEquals(4, ingress.snapshotsScheduled());
    }

    @Test
    void countsAndSkipsMalformedMessages()
    {
        final CommandIngress ingress = new CommandIngress(null, ring, clock, 10, new int[] {1}, 0);
        ingress.onMessage(buffer, 0, 3);                                       // shorter than a header
        buffer.putShort(4, (short)42, MessageHeader.BYTE_ORDER);               // foreign schema
        ingress.onMessage(buffer, 0, 64);
        new ExecutionReportFlyweight().wrapForEncode(buffer, 0);               // not a command
        ingress.onMessage(buffer, 0, ExecutionReportFlyweight.ENCODED_LENGTH);
        new NewOrderFlyweight().wrapForEncode(buffer, 0);                      // truncated
        ingress.onMessage(buffer, 0, 20);
        new CancelOrderFlyweight().wrapForEncode(buffer, 0);
        ingress.onMessage(buffer, 0, 20);
        new BookSnapshotRequestFlyweight().wrapForEncode(buffer, 0);
        ingress.onMessage(buffer, 0, 12);
        assertEquals(6, ingress.malformedMessages());
        assertEquals(-1, ring.getCursor(), "nothing published");
    }

    @Test
    void periodicSnapshotsForEveryInstrument()
    {
        final CommandIngress ingress = new CommandIngress(null, ring, clock, 10, new int[] {1, 2}, 500);
        assertEquals(0, ingress.doWork());
        now[0] += 499;
        assertEquals(0, ingress.doWork());
        now[0] += 1;
        assertEquals(1, ingress.doWork());
        assertEquals(1, ring.getCursor());
        assertEquals(CommandType.BOOK_SNAPSHOT, ring.get(0).type());
        assertEquals(1, ring.get(0).instrumentId());
        assertEquals(2, ring.get(1).instrumentId());
        assertEquals(0, ingress.doWork());
        now[0] += 500;
        assertEquals(1, ingress.doWork());
        assertTrue(ingress.snapshotsScheduled() == 4);

        final CommandIngress disabled = new CommandIngress(null, ring, clock, 10, new int[] {1}, 0);
        now[0] += 10_000_000;
        assertEquals(0, disabled.doWork());
    }
}
