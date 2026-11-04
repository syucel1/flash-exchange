package com.flashexchange.protocol;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EngineMessageDispatcherTest
{
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    final EngineMessageDispatcher dispatcher = new EngineMessageDispatcher();
    final List<String> seen = new ArrayList<>();
    final EngineEventListener recorder = new EngineEventListener()
    {
        @Override
        public void onExecutionReport(final ExecutionReportFlyweight report)
        {
            seen.add("report:" + report.orderId());
        }

        @Override
        public void onTradeExecution(final TradeExecutionFlyweight trade)
        {
            seen.add("trade:" + trade.tradeId());
        }

        @Override
        public void onBookUpdate(final BookUpdateFlyweight update)
        {
            seen.add("update:" + update.bookSequence());
        }

        @Override
        public void onBookSnapshot(final BookSnapshotFlyweight snapshot)
        {
            seen.add("snapshot:" + snapshot.bookSequence());
        }

        @Override
        public void onPublicTrade(final PublicTradeFlyweight trade)
        {
            seen.add("public:" + trade.tradeId());
        }

        @Override
        public void onUnknownMessage(final int templateId)
        {
            seen.add("unknown:" + templateId);
        }
    };

    @Test
    void dispatchesEveryOutputMessage()
    {
        assertEquals(ExecutionReportFlyweight.TEMPLATE_ID, dispatcher.dispatch(buffer, 0,
            new ExecutionReportFlyweight().wrapForEncode(buffer, 0).orderId(1).encodedLength(), recorder));
        assertEquals(TradeExecutionFlyweight.TEMPLATE_ID, dispatcher.dispatch(buffer, 0,
            new TradeExecutionFlyweight().wrapForEncode(buffer, 0).tradeId(2).encodedLength(), recorder));
        assertEquals(BookUpdateFlyweight.TEMPLATE_ID, dispatcher.dispatch(buffer, 0,
            new BookUpdateFlyweight().wrapForEncode(buffer, 0).bookSequence(3).levelCount(0).encodedLength(),
            recorder));
        assertEquals(BookSnapshotFlyweight.TEMPLATE_ID, dispatcher.dispatch(buffer, 0,
            new BookSnapshotFlyweight().wrapForEncode(buffer, 0).bookSequence(4).bidCount(0).askCount(0)
                .encodedLength(), recorder));
        assertEquals(PublicTradeFlyweight.TEMPLATE_ID, dispatcher.dispatch(buffer, 0,
            new PublicTradeFlyweight().wrapForEncode(buffer, 0).tradeId(5).encodedLength(), recorder));
        assertEquals(List.of("report:1", "trade:2", "update:3", "snapshot:4", "public:5"), seen);
    }

    @Test
    void reportsUnknownAndMalformedMessages()
    {
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 3, recorder));
        new NewOrderFlyweight().wrapForEncode(buffer, 0);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, NewOrderFlyweight.ENCODED_LENGTH, recorder));
        new ExecutionReportFlyweight().wrapForEncode(buffer, 0);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 20, recorder));
        new TradeExecutionFlyweight().wrapForEncode(buffer, 0);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 20, recorder));
        new BookUpdateFlyweight().wrapForEncode(buffer, 0);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 20, recorder));
        new BookSnapshotFlyweight().wrapForEncode(buffer, 0);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 20, recorder));
        new PublicTradeFlyweight().wrapForEncode(buffer, 0);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 20, recorder));
        buffer.putShort(4, (short)1, MessageHeader.BYTE_ORDER);
        assertEquals(-1, dispatcher.dispatch(buffer, 0, 64, recorder));
        assertEquals(List.of("unknown:-1", "unknown:1", "unknown:101", "unknown:102", "unknown:201",
            "unknown:202", "unknown:203", "unknown:-1"), seen);

        final EngineEventListener noop = new EngineEventListener()
        {
        };
        noop.onExecutionReport(null);
        noop.onTradeExecution(null);
        noop.onBookUpdate(null);
        noop.onBookSnapshot(null);
        noop.onPublicTrade(null);
        noop.onUnknownMessage(0);
    }
}
