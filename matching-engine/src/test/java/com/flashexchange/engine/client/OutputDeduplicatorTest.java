package com.flashexchange.engine.client;

import com.flashexchange.protocol.BookSnapshotFlyweight;
import com.flashexchange.protocol.EngineRoleFlyweight;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.NodeRole;
import com.flashexchange.protocol.PublicTradeFlyweight;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputDeduplicatorTest
{
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    final OutputDeduplicator dedup = new OutputDeduplicator();

    boolean message(final long sequence)
    {
        new PublicTradeFlyweight().wrapForEncode(buffer, 0).engineSequence(sequence);
        return dedup.accept(buffer, 0, PublicTradeFlyweight.ENCODED_LENGTH);
    }

    boolean role(final long epoch)
    {
        new EngineRoleFlyweight().wrapForEncode(buffer, 0).epoch(epoch).role(NodeRole.PRIMARY);
        return dedup.accept(buffer, 0, EngineRoleFlyweight.ENCODED_LENGTH);
    }

    boolean snapshot()
    {
        MessageHeader.write(buffer, 0, 32, BookSnapshotFlyweight.TEMPLATE_ID);
        return dedup.accept(buffer, 0, MessageHeader.LENGTH + 32);
    }

    @Test
    void dropsExactlyTheRepeatedPrefixAfterAnEpochChange()
    {
        assertTrue(role(1));
        final List<Boolean> first = new ArrayList<>();
        for (final long seq : new long[] {0, 0, 1, 2, 2, 2})
        {
            first.add(message(seq));
        }
        assertEquals(List.of(true, true, true, true, true, true), first);

        // failover: the new primary re-publishes 1, 2, 2, 2 then continues with a fourth message for 2 and 3
        assertTrue(role(2));
        assertTrue(snapshot(), "snapshots always pass");
        final List<Boolean> replay = new ArrayList<>();
        for (final long seq : new long[] {1, 2, 2, 2, 2, 3, 3})
        {
            replay.add(message(seq));
        }
        assertEquals(List.of(false, false, false, false, true, true, true), replay);
        assertEquals(4, dedup.duplicatesDropped());
        assertEquals(3, dedup.lastSequence());
        assertEquals(2, dedup.lastEpoch());

        // same or older epoch announcements do not re-arm the filter
        assertTrue(role(2));
        assertTrue(message(3));
    }

    @Test
    void firstRoleMessageCountsWhenOutputWasAlreadyReceived()
    {
        assertTrue(message(5));
        assertTrue(role(7));
        assertEquals(false, message(5));
        assertTrue(message(6));
        assertTrue(dedup.accept(buffer, 0, 4), "short messages are passed through");
    }

    @Test
    void noFilteringForAFreshConsumer()
    {
        assertTrue(role(3));
        assertTrue(message(10));
        assertEquals(0, dedup.duplicatesDropped());
    }
}
