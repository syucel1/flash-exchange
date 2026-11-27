package com.flashexchange.engine.client;

import com.flashexchange.protocol.BookSnapshotFlyweight;
import com.flashexchange.protocol.EngineRoleFlyweight;
import com.flashexchange.protocol.MessageHeader;
import org.agrona.DirectBuffer;

/**
 * Drops engine output a consumer has already seen. After a recovery or failover the engine re-publishes the output
 * of every command it cannot prove was released (at-least-once). Output is deterministic and ordered by engine
 * sequence, and every message except {@code EngineRole} carries its command's engine sequence in the first eight
 * bytes of its body, so a consumer that remembers the last sequence it saw and how many messages it received for it
 * can skip exactly the repeated prefix after an {@code EngineRole} with a newer epoch (the first role message a
 * consumer sees also counts when it already received output, e.g. it connected after the engine started).
 * <p>
 * Book snapshots reuse the last sequence and are self-contained, so they are never counted or dropped. One instance
 * per output stream; not thread safe.
 */
public final class OutputDeduplicator
{
    private long lastEpoch = Long.MIN_VALUE;
    private long lastSequence = Long.MIN_VALUE;
    private int countAtLastSequence;
    private boolean filtering;
    private int repeatedAtLastSequence;
    private long duplicatesDropped;

    /**
     * Decide whether a message should be delivered.
     *
     * @param buffer containing the message.
     * @param offset of the message header.
     * @param length of the message.
     * @return true to deliver, false if it is a duplicate.
     */
    public boolean accept(final DirectBuffer buffer, final int offset, final int length)
    {
        if (length < MessageHeader.LENGTH + Long.BYTES)
        {
            return true;
        }
        final int templateId = MessageHeader.templateId(buffer, offset);
        if (templateId == EngineRoleFlyweight.TEMPLATE_ID)
        {
            final long epoch = buffer.getLong(offset + MessageHeader.LENGTH, MessageHeader.BYTE_ORDER);
            if (epoch > lastEpoch)
            {
                if (lastSequence != Long.MIN_VALUE)
                {
                    filtering = true;
                    repeatedAtLastSequence = 0;
                }
                lastEpoch = epoch;
            }
            return true;
        }
        if (templateId == BookSnapshotFlyweight.TEMPLATE_ID)
        {
            return true;
        }

        final long sequence = buffer.getLong(offset + MessageHeader.LENGTH, MessageHeader.BYTE_ORDER);
        if (filtering)
        {
            if (sequence < lastSequence)
            {
                duplicatesDropped++;
                return false;
            }
            if (sequence == lastSequence && repeatedAtLastSequence < countAtLastSequence)
            {
                repeatedAtLastSequence++;
                duplicatesDropped++;
                return false;
            }
            filtering = false;
        }
        if (sequence == lastSequence)
        {
            countAtLastSequence++;
        }
        else
        {
            lastSequence = sequence;
            countAtLastSequence = 1;
        }
        return true;
    }

    /**
     * Highest engine sequence delivered.
     *
     * @return sequence, {@link Long#MIN_VALUE} before the first.
     */
    public long lastSequence()
    {
        return lastSequence;
    }

    /**
     * Highest epoch seen.
     *
     * @return epoch, {@link Long#MIN_VALUE} before the first role message.
     */
    public long lastEpoch()
    {
        return lastEpoch;
    }

    /**
     * Messages dropped as duplicates.
     *
     * @return count.
     */
    public long duplicatesDropped()
    {
        return duplicatesDropped;
    }
}
