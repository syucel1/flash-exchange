package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.matching.MatchingEngine;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.JournalEntryFlyweight;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.NodeRole;
import com.flashexchange.protocol.Reason;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableCommandTest
{
    @Test
    void journalRoundTripPreservesEverything()
    {
        final OrderCommand original = new OrderCommand()
            .newOrder(11, 22, 3, (byte)2, (byte)1, (byte)3, (byte)1, true, 105, 7)
            .stamp(40, 999)
            .epoch(5)
            .markDuplicate();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final JournalEntryFlyweight entry = new JournalEntryFlyweight().wrapForEncode(buffer, 0);
        original.toJournal(entry, 37);
        assertEquals(JournalEntryFlyweight.ENCODED_LENGTH, entry.encodedLength());

        final OrderCommand copy = new OrderCommand().fromJournal(
            new JournalEntryFlyweight().wrapForDecode(buffer, 0), true);
        assertEquals(CommandType.NEW_ORDER, copy.type());
        assertEquals(40, copy.engineSequence());
        assertEquals(999, copy.timestamp());
        assertEquals(41, copy.orderId());
        assertEquals(11, copy.clientOrderId());
        assertEquals(22, copy.userId());
        assertEquals(5, copy.epoch());
        assertEquals(37, copy.releasedSequence());
        assertTrue(copy.isDuplicate());
        assertTrue(copy.isReplay());
        assertTrue(copy.postOnly);
        assertEquals(105, copy.price);
        assertEquals(7, copy.quantity);
        assertEquals(3, copy.instrumentId());
        assertEquals((byte)2, copy.side);
        assertEquals((byte)1, copy.orderType);
        assertEquals((byte)3, copy.timeInForce);
        assertEquals((byte)1, copy.selfTradePrevention);

        new OrderCommand().cancelOrder(1, 2, 3, 4).stamp(9, 9).toJournal(entry, -1);
        final OrderCommand cancel = new OrderCommand().fromJournal(entry, false);
        assertEquals(CommandType.CANCEL_ORDER, cancel.type());
        assertEquals(4, cancel.orderId());
        assertFalse(cancel.isDuplicate());
        assertFalse(cancel.isReplay());

        entry.commandType(CommandType.BOOK_SNAPSHOT.code());
        assertThrows(IllegalArgumentException.class, () -> new OrderCommand().fromJournal(entry, false));
    }

    @Test
    void populateResetsDurabilityFlags()
    {
        final OrderCommand slot = new OrderCommand().roleChange(NodeRole.PRIMARY, 3);
        assertEquals(NodeRole.PRIMARY, slot.role());
        assertEquals(3, slot.epoch());
        assertEquals(0, slot.sectionCount());
        slot.journalPosition(512);
        slot.markDuplicate();
        slot.newOrder(1, 1, 1, (byte)1, (byte)1, (byte)1, (byte)0, false, 1, 1);
        assertEquals(0, slot.journalPosition());
        assertFalse(slot.isDuplicate());
        assertNull(slot.role());
        assertEquals(-1, slot.releasedSequence());

        slot.stateSnapshot(3);
        assertEquals(3, slot.sectionCount());
        slot.section(2).putLong(1000, 5);
        slot.sectionLength(2, 1008);
        slot.stateSnapshot(3);
        assertEquals(0, slot.sectionLength(2), "lengths reset, buffers reused");
        assertEquals(5, slot.section(2).getLong(1000));
        slot.stateSnapshot(2);
        assertEquals(2, slot.sectionCount());

        assertTrue(CommandType.NEW_ORDER.isJournaled());
        assertTrue(CommandType.CANCEL_ORDER.isJournaled());
        assertFalse(CommandType.BOOK_SNAPSHOT.isJournaled());
        assertFalse(CommandType.STATE_SNAPSHOT.isJournaled());
        assertFalse(CommandType.ROLE_CHANGE.isJournaled());
    }

    @Test
    void matcherHandlesDuplicatesSnapshotsAndRoleChanges()
    {
        final EncodingEventSink sink = new EncodingEventSink();
        final MatchingEngine engine = new MatchingEngine(
            List.of(new Instrument(3, "A", 1, 1, 1, 100, 0)), sink, 4, 0);
        final MatcherEventHandler handler = new MatcherEventHandler(1, 2, engine, sink);

        final OrderCommand resting = new OrderCommand().newOrder(1, 1, 3, (byte)1, (byte)1, (byte)1, (byte)0,
            false, 10, 1).stamp(0, 0);
        handler.onEvent(resting, 0, true);
        assertEquals(1, engine.book(3).orderCount());

        final OrderCommand dup = new OrderCommand().newOrder(1, 1, 3, (byte)1, (byte)1, (byte)1, (byte)0,
            false, 10, 1).stamp(1, 0).markDuplicate();
        handler.onEvent(dup, 1, true);
        assertEquals(1, dup.executionOutput().count());
        assertEquals(0, dup.marketDataOutput().count());
        final ExecutionReportFlyweight report = new ExecutionReportFlyweight()
            .wrapForDecode(dup.executionOutput().buffer(), 0);
        assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, report.reason());
        assertEquals(1, engine.book(3).orderCount(), "duplicate does not touch the book");
        assertEquals(ExecutionReportFlyweight.TEMPLATE_ID,
            MessageHeader.templateId(dup.executionOutput().buffer(), 0));

        final OrderCommand snapshot = new OrderCommand().stateSnapshot(3);
        handler.onEvent(snapshot, 2, true);
        assertEquals(0, snapshot.sectionLength(1), "shard 0 is another handler");
        assertTrue(snapshot.sectionLength(2) > 0);
        final MatchingEngine restored = new MatchingEngine(
            List.of(new Instrument(3, "A", 1, 1, 1, 100, 0)), new EncodingEventSink(), 4, 0);
        restored.readState(snapshot.section(2), 0);
        assertEquals(1, restored.book(3).orderCount());

        final OrderCommand role = new OrderCommand().roleChange(NodeRole.PRIMARY, 2);
        handler.onEvent(role, 3, true);
        assertEquals(0, role.executionOutput().count());
    }
}
