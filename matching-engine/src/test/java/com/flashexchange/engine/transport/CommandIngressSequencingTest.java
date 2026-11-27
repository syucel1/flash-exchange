package com.flashexchange.engine.transport;

import com.flashexchange.engine.ha.Sequencer;
import com.flashexchange.engine.pipeline.CommandType;
import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.CancelOrderFlyweight;
import com.flashexchange.protocol.JournalEntryFlyweight;
import com.flashexchange.protocol.NewOrderFlyweight;
import com.flashexchange.protocol.NodeRole;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import com.lmax.disruptor.RingBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandIngressSequencingTest
{
    final RingBuffer<OrderCommand> ring = RingBuffer.createSingleProducer(OrderCommand::new, 64);
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    final Sequencer sequencer = new Sequencer(64);
    final CommandIngress ingress = new CommandIngress(null, ring, () -> 5L, 10, new int[] {1}, 0, sequencer);

    private void newOrder(final long clientOrderId)
    {
        new NewOrderFlyweight().wrapForEncode(buffer, 0).clientOrderId(clientOrderId).userId(8).instrumentId(1)
            .side(Side.BUY).orderType(OrderType.LIMIT).timeInForce(TimeInForce.GTC).price(10).quantity(1);
        ingress.onMessage(buffer, 0, NewOrderFlyweight.ENCODED_LENGTH);
    }

    @Test
    void onlyStateChangingCommandsConsumeSequences()
    {
        ingress.publishRoleChange(NodeRole.STANDALONE, 4);
        assertEquals(CommandType.ROLE_CHANGE, ring.get(0).type());
        assertEquals(-1, ring.get(0).engineSequence());
        assertEquals(4, ingress.epoch());

        newOrder(1);
        new BookSnapshotRequestFlyweight().wrapForEncode(buffer, 0).instrumentId(1);
        ingress.onMessage(buffer, 0, BookSnapshotRequestFlyweight.ENCODED_LENGTH);
        new CancelOrderFlyweight().wrapForEncode(buffer, 0).clientOrderId(2).userId(8).instrumentId(1).orderId(1);
        ingress.onMessage(buffer, 0, CancelOrderFlyweight.ENCODED_LENGTH);

        assertEquals(0, ring.get(1).engineSequence());
        assertEquals(1, ring.get(1).orderId());
        assertEquals(4, ring.get(1).epoch());
        assertEquals(CommandType.BOOK_SNAPSHOT, ring.get(2).type());
        assertEquals(0, ring.get(2).engineSequence(), "book snapshots reuse the last sequence");
        assertEquals(1, ring.get(3).engineSequence());
        assertEquals(4, ring.get(3).epoch());
    }

    @Test
    void flagsDuplicateClientOrderIds()
    {
        newOrder(77);
        newOrder(77);
        newOrder(78);
        assertFalse(ring.get(0).isDuplicate());
        assertTrue(ring.get(1).isDuplicate());
        assertEquals(1, ring.get(1).engineSequence(), "duplicates are still sequenced (and journaled)");
        assertFalse(ring.get(2).isDuplicate());
        assertEquals(1, ingress.duplicatesDetected());
    }

    @Test
    void publishesJournalEntriesAndSnapshots()
    {
        final JournalEntryFlyweight entry = new JournalEntryFlyweight().wrapForEncode(buffer, 0);
        new OrderCommand().newOrder(9, 8, 1, (byte)1, (byte)1, (byte)1, (byte)0, false, 10, 1).stamp(41, 3)
            .toJournal(entry, 40);
        ingress.publishJournalEntry(entry, true);
        assertEquals(41, sequencer.lastSequence());
        assertTrue(ring.get(0).isReplay());
        assertEquals(42, ring.get(0).orderId());

        newOrder(9);
        assertTrue(ring.get(1).isDuplicate(), "replayed orders enter the dedup window");
        assertEquals(42, ring.get(1).engineSequence());

        ingress.publishStateSnapshot(2);
        final OrderCommand snapshot = ring.get(2);
        assertEquals(CommandType.STATE_SNAPSHOT, snapshot.type());
        assertEquals(3, snapshot.sectionCount());
        assertEquals(42, snapshot.engineSequence());
        final Sequencer restored = new Sequencer(64);
        assertEquals(snapshot.sectionLength(0), restored.read(snapshot.section(0), 0));
        assertEquals(42, restored.lastSequence());
        assertEquals(0, ingress.discard());
        assertEquals(0, ingress.commandsDiscarded());
    }
}
