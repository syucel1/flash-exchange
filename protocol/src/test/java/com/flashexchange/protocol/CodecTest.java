package com.flashexchange.protocol;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodecTest
{
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 17})
    void newOrderRoundTrip(final int offset)
    {
        final NewOrderFlyweight enc = new NewOrderFlyweight().wrapForEncode(buffer, offset)
            .clientOrderId(11).userId(22).price(33).quantity(44).sendingTime(55).instrumentId(66)
            .side(Side.SELL).orderType(OrderType.MARKET).timeInForce(TimeInForce.FOK)
            .selfTradePrevention(SelfTradePrevention.CANCEL_BOTH).flags(NewOrderFlyweight.FLAG_POST_ONLY);
        assertEquals(NewOrderFlyweight.ENCODED_LENGTH, enc.encodedLength());
        assertEquals(offset, enc.messageOffset());
        assertSame(buffer, enc.buffer());

        assertTrue(MessageHeader.isValid(buffer, offset, enc.encodedLength(), NewOrderFlyweight.BLOCK_LENGTH));
        assertEquals(NewOrderFlyweight.TEMPLATE_ID, MessageHeader.templateId(buffer, offset));
        assertEquals(MessageHeader.SCHEMA_ID, MessageHeader.schemaId(buffer, offset));
        assertEquals(MessageHeader.SCHEMA_VERSION, MessageHeader.version(buffer, offset));
        assertEquals(NewOrderFlyweight.BLOCK_LENGTH, MessageHeader.blockLength(buffer, offset));

        final NewOrderFlyweight dec = new NewOrderFlyweight().wrapForDecode(buffer, offset);
        assertEquals(11, dec.clientOrderId());
        assertEquals(22, dec.userId());
        assertEquals(33, dec.price());
        assertEquals(44, dec.quantity());
        assertEquals(55, dec.sendingTime());
        assertEquals(66, dec.instrumentId());
        assertEquals(Side.SELL, dec.side());
        assertEquals(OrderType.MARKET, dec.orderType());
        assertEquals(TimeInForce.FOK, dec.timeInForce());
        assertEquals(SelfTradePrevention.CANCEL_BOTH, dec.selfTradePrevention());
        assertTrue(dec.isPostOnly());
        assertEquals(Side.SELL.code(), dec.sideCode());

        enc.flags((byte)0).sideCode((byte)99);
        assertFalse(dec.isPostOnly());
        assertNull(dec.side());
        assertEquals(99, dec.sideCode());
    }

    @Test
    void cancelAndSnapshotRequestRoundTrip()
    {
        new CancelOrderFlyweight().wrapForEncode(buffer, 0)
            .clientOrderId(1).userId(2).orderId(3).sendingTime(4).instrumentId(5);
        final CancelOrderFlyweight cancel = new CancelOrderFlyweight().wrapForDecode(buffer, 0);
        assertEquals(1, cancel.clientOrderId());
        assertEquals(2, cancel.userId());
        assertEquals(3, cancel.orderId());
        assertEquals(4, cancel.sendingTime());
        assertEquals(5, cancel.instrumentId());
        assertEquals(CancelOrderFlyweight.TEMPLATE_ID, cancel.templateId());
        assertEquals(CancelOrderFlyweight.BLOCK_LENGTH, cancel.blockLength());

        new BookSnapshotRequestFlyweight().wrapForEncode(buffer, 0).correlationId(9).instrumentId(8);
        final BookSnapshotRequestFlyweight req = new BookSnapshotRequestFlyweight().wrapForDecode(buffer, 0);
        assertEquals(9, req.correlationId());
        assertEquals(8, req.instrumentId());
        assertEquals(BookSnapshotRequestFlyweight.ENCODED_LENGTH, req.encodedLength());
    }

    @Test
    void executionReportRoundTrip()
    {
        new ExecutionReportFlyweight().wrapForEncode(buffer, 0)
            .engineSequence(1).timestamp(2).orderId(3).clientOrderId(4).userId(5).price(6).quantity(7)
            .lastPrice(8).lastQuantity(9).cumulativeQuantity(10).leavesQuantity(11).tradeId(12).instrumentId(13)
            .execType(ExecType.TRADE).orderStatus(OrderStatus.PARTIALLY_FILLED).side(Side.BUY)
            .orderType(OrderType.LIMIT).timeInForce(TimeInForce.GTC).liquidity(Liquidity.MAKER)
            .reason(Reason.NONE);
        final ExecutionReportFlyweight r = new ExecutionReportFlyweight().wrapForDecode(buffer, 0);
        assertEquals(1, r.engineSequence());
        assertEquals(2, r.timestamp());
        assertEquals(3, r.orderId());
        assertEquals(4, r.clientOrderId());
        assertEquals(5, r.userId());
        assertEquals(6, r.price());
        assertEquals(7, r.quantity());
        assertEquals(8, r.lastPrice());
        assertEquals(9, r.lastQuantity());
        assertEquals(10, r.cumulativeQuantity());
        assertEquals(11, r.leavesQuantity());
        assertEquals(12, r.tradeId());
        assertEquals(13, r.instrumentId());
        assertEquals(ExecType.TRADE, r.execType());
        assertEquals(OrderStatus.PARTIALLY_FILLED, r.orderStatus());
        assertEquals(Side.BUY, r.side());
        assertEquals(OrderType.LIMIT, r.orderType());
        assertEquals(TimeInForce.GTC, r.timeInForce());
        assertEquals(Liquidity.MAKER, r.liquidity());
        assertEquals(Reason.NONE, r.reason());
        assertEquals(ExecType.TRADE.code(), r.execTypeCode());
    }

    @Test
    void tradeMessagesRoundTrip()
    {
        new TradeExecutionFlyweight().wrapForEncode(buffer, 0)
            .engineSequence(1).timestamp(2).tradeId(3).price(4).quantity(5).buyOrderId(6).sellOrderId(7)
            .buyUserId(8).sellUserId(9).buyClientOrderId(10).sellClientOrderId(11).instrumentId(12)
            .aggressorSide(Side.SELL);
        final TradeExecutionFlyweight t = new TradeExecutionFlyweight().wrapForDecode(buffer, 0);
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L), List.of(t.engineSequence(), t.timestamp(),
            t.tradeId(), t.price(), t.quantity(), t.buyOrderId(), t.sellOrderId(), t.buyUserId(), t.sellUserId(),
            t.buyClientOrderId(), t.sellClientOrderId()));
        assertEquals(12, t.instrumentId());
        assertEquals(Side.SELL, t.aggressorSide());

        new PublicTradeFlyweight().wrapForEncode(buffer, 0)
            .engineSequence(1).timestamp(2).tradeId(3).price(4).quantity(5).instrumentId(6).aggressorSide(Side.BUY);
        final PublicTradeFlyweight p = new PublicTradeFlyweight().wrapForDecode(buffer, 0);
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), List.of(p.engineSequence(), p.timestamp(), p.tradeId(), p.price(),
            p.quantity()));
        assertEquals(6, p.instrumentId());
        assertEquals(Side.BUY, p.aggressorSide());
    }

    @Test
    void bookUpdateRoundTrip()
    {
        final BookUpdateFlyweight enc = new BookUpdateFlyweight().wrapForEncode(buffer, 5)
            .engineSequence(1).timestamp(2).bookSequence(3).instrumentId(4).levelCount(2)
            .putLevel(0, Side.BUY, 100, 10, 1)
            .putLevel(1, Side.SELL, 101, 0, 0);
        assertEquals(BookUpdateFlyweight.encodedLength(2), enc.encodedLength());

        final BookUpdateFlyweight dec = new BookUpdateFlyweight().wrapForDecode(buffer, 5);
        assertEquals(1, dec.engineSequence());
        assertEquals(2, dec.timestamp());
        assertEquals(3, dec.bookSequence());
        assertEquals(4, dec.instrumentId());
        assertEquals(2, dec.levelCount());
        assertEquals(Side.BUY, dec.levelSide(0));
        assertEquals(100, dec.levelPrice(0));
        assertEquals(10, dec.levelQuantity(0));
        assertEquals(1, dec.levelOrderCount(0));
        assertEquals(Side.SELL, dec.levelSide(1));
        assertEquals(0, dec.levelQuantity(1));
        assertEquals(BookUpdateFlyweight.encodedLength(2), dec.encodedLength());
        assertThrows(IndexOutOfBoundsException.class, () -> dec.levelPrice(2));
        assertThrows(IndexOutOfBoundsException.class, () -> dec.levelPrice(-1));
        assertThrows(IllegalArgumentException.class, () -> enc.levelCount(-1));
        assertThrows(IllegalArgumentException.class, () -> enc.levelCount(BookLevelsFlyweight.MAX_ENTRIES + 1));
    }

    @Test
    void bookSnapshotRoundTrip()
    {
        final BookSnapshotFlyweight enc = new BookSnapshotFlyweight().wrapForEncode(buffer, 0)
            .engineSequence(1).timestamp(2).bookSequence(3).instrumentId(4)
            .bidCount(2).putBid(0, 100, 10, 1).putBid(1, 99, 20, 2)
            .askCount(1).putAsk(0, 101, 5, 1);
        assertEquals(BookSnapshotFlyweight.encodedLength(2, 1), enc.encodedLength());

        final BookSnapshotFlyweight dec = new BookSnapshotFlyweight().wrapForDecode(buffer, 0);
        assertEquals(BookSnapshotFlyweight.TEMPLATE_ID, dec.templateId());
        assertEquals(3, dec.bookSequence());
        assertEquals(2, dec.bidCount());
        assertEquals(1, dec.askCount());
        assertEquals(99, dec.bidPrice(1));
        assertEquals(20, dec.bidQuantity(1));
        assertEquals(2, dec.bidOrderCount(1));
        assertEquals(101, dec.askPrice(0));
        assertEquals(5, dec.askQuantity(0));
        assertEquals(1, dec.askOrderCount(0));
        assertEquals(enc.encodedLength(), dec.encodedLength());

        final BookSnapshotFlyweight empty = new BookSnapshotFlyweight().wrapForEncode(buffer, 0).bidCount(0)
            .askCount(0);
        assertEquals(BookSnapshotFlyweight.encodedLength(0, 0), empty.encodedLength());
    }

    @Test
    void headerValidation()
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        new CancelOrderFlyweight().wrapForEncode(b, 0);
        assertTrue(MessageHeader.isValid(b, 0, CancelOrderFlyweight.ENCODED_LENGTH, CancelOrderFlyweight.BLOCK_LENGTH));
        assertFalse(MessageHeader.isValid(b, 0, 4, CancelOrderFlyweight.BLOCK_LENGTH), "shorter than header");
        assertFalse(MessageHeader.isValid(b, 0, CancelOrderFlyweight.ENCODED_LENGTH - 1,
            CancelOrderFlyweight.BLOCK_LENGTH), "truncated block");
        assertFalse(MessageHeader.isValid(b, 0, 128, CancelOrderFlyweight.BLOCK_LENGTH + 8), "block too small");
        b.putShort(4, (short)1, MessageHeader.BYTE_ORDER);
        assertFalse(MessageHeader.isValid(b, 0, 128, CancelOrderFlyweight.BLOCK_LENGTH), "wrong schema");
    }

    @Test
    void enumsRoundTripAndRejectUnknownCodes()
    {
        final List<Enum<?>[]> all = new ArrayList<>();
        all.add(Side.values());
        all.add(OrderType.values());
        all.add(TimeInForce.values());
        all.add(ExecType.values());
        all.add(OrderStatus.values());
        all.add(Liquidity.values());
        all.add(SelfTradePrevention.values());
        all.add(Reason.values());
        for (final Side v : Side.values())
        {
            assertSame(v, Side.get(v.code()));
        }
        for (final OrderType v : OrderType.values())
        {
            assertSame(v, OrderType.get(v.code()));
        }
        for (final TimeInForce v : TimeInForce.values())
        {
            assertSame(v, TimeInForce.get(v.code()));
        }
        for (final ExecType v : ExecType.values())
        {
            assertSame(v, ExecType.get(v.code()));
        }
        for (final OrderStatus v : OrderStatus.values())
        {
            assertSame(v, OrderStatus.get(v.code()));
        }
        for (final Liquidity v : Liquidity.values())
        {
            assertSame(v, Liquidity.get(v.code()));
        }
        for (final SelfTradePrevention v : SelfTradePrevention.values())
        {
            assertSame(v, SelfTradePrevention.get(v.code()));
        }
        for (final Reason v : Reason.values())
        {
            assertSame(v, Reason.get(v.code()));
        }
        assertNull(Side.get((byte)0));
        assertNull(OrderType.get((byte)-1));
        assertNull(TimeInForce.get((byte)0));
        assertNull(ExecType.get((byte)0));
        assertNull(OrderStatus.get((byte)0));
        assertNull(Liquidity.get((byte)7));
        assertNull(SelfTradePrevention.get((byte)7));
        assertNull(Reason.get((byte)200));
        assertEquals(8, all.size());
        assertSame(Side.SELL, Side.BUY.opposite());
        assertSame(Side.BUY, Side.SELL.opposite());
    }
}
