package com.flashexchange.protocol;

import org.agrona.DirectBuffer;

/**
 * One section of a state snapshot (stream 110): the sequencer state (section 0) or the state of one matcher shard
 * (section 1..n). Variable length: an 8 byte root block followed by {@link #payloadLength()} payload bytes.
 * <pre>
 *      0  4  section
 *      4  4  payloadLength
 *      8  .. payload
 * </pre>
 */
public final class SnapshotChunkFlyweight extends Flyweight<SnapshotChunkFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 61;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 8;

    private static final int SECTION_OFFSET = 0;
    private static final int PAYLOAD_LENGTH_OFFSET = 4;

    @Override
    public int templateId()
    {
        return TEMPLATE_ID;
    }

    @Override
    public int blockLength()
    {
        return BLOCK_LENGTH;
    }

    /**
     * Section index.
     *
     * @return section.
     */
    public int section()
    {
        return getInt(SECTION_OFFSET);
    }

    /**
     * Payload length.
     *
     * @return bytes.
     */
    public int payloadLength()
    {
        return getInt(PAYLOAD_LENGTH_OFFSET);
    }

    /**
     * Offset of the payload in the wrapped buffer.
     *
     * @return absolute offset.
     */
    public int payloadOffset()
    {
        return offset + actingBlockLength;
    }

    /**
     * Encode the section header and copy the payload.
     *
     * @param section index.
     * @param source  payload buffer.
     * @param offset  payload offset in {@code source}.
     * @param length  payload length.
     * @return this for fluent use.
     */
    public SnapshotChunkFlyweight put(
        final int section, final DirectBuffer source, final int offset, final int length)
    {
        putInt(SECTION_OFFSET, section);
        putInt(PAYLOAD_LENGTH_OFFSET, length);
        mutableBuffer.putBytes(payloadOffset(), source, offset, length);
        return this;
    }

    @Override
    public int encodedLength()
    {
        return MessageHeader.LENGTH + actingBlockLength + payloadLength();
    }

    /**
     * Encoded length for a payload size.
     *
     * @param payloadLength payload bytes.
     * @return total message length.
     */
    public static int encodedLength(final int payloadLength)
    {
        return MessageHeader.LENGTH + BLOCK_LENGTH + payloadLength;
    }
}
