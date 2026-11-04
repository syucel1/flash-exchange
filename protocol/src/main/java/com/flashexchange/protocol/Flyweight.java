package com.flashexchange.protocol;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import static com.flashexchange.protocol.MessageHeader.BYTE_ORDER;

/**
 * Base class of the hand-written, zero-allocation message flyweights.
 * <p>
 * A flyweight is wrapped over a buffer either for encoding ({@link #wrapForEncode(MutableDirectBuffer, int)}, which
 * also writes the {@link MessageHeader}) or for decoding ({@link #wrapForDecode(DirectBuffer, int)}). Instances are
 * reusable and not thread safe. Field offsets are relative to the start of the root block (after the header).
 *
 * @param <T> concrete flyweight type, returned by the wrap methods for fluent use.
 */
public abstract class Flyweight<T extends Flyweight<T>>
{
    /** Buffer being read. */
    protected DirectBuffer buffer;

    /** Same buffer as {@link #buffer} when wrapped for encoding, otherwise {@code null}. */
    protected MutableDirectBuffer mutableBuffer;

    /** Offset of the start of the message header. */
    protected int messageOffset;

    /** Offset of the start of the root block. */
    protected int offset;

    /** Block length used to locate data after the root block. */
    protected int actingBlockLength;

    /**
     * Template id of this message.
     *
     * @return template id.
     */
    public abstract int templateId();

    /**
     * Length of the fixed root block.
     *
     * @return block length in bytes.
     */
    public abstract int blockLength();

    /**
     * Wrap a buffer for encoding and write the message header.
     *
     * @param buffer to encode into.
     * @param offset at which the message (header) starts.
     * @return this for fluent use.
     */
    @SuppressWarnings("unchecked")
    public T wrapForEncode(final MutableDirectBuffer buffer, final int offset)
    {
        MessageHeader.write(buffer, offset, blockLength(), templateId());
        this.buffer = buffer;
        this.mutableBuffer = buffer;
        this.messageOffset = offset;
        this.offset = offset + MessageHeader.LENGTH;
        this.actingBlockLength = blockLength();
        return (T)this;
    }

    /**
     * Wrap a buffer for decoding. The caller is expected to have checked the header with
     * {@link MessageHeader#isValid(DirectBuffer, int, int, int)}.
     *
     * @param buffer to decode from.
     * @param offset at which the message (header) starts.
     * @return this for fluent use.
     */
    @SuppressWarnings("unchecked")
    public T wrapForDecode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.mutableBuffer = null;
        this.messageOffset = offset;
        this.offset = offset + MessageHeader.LENGTH;
        this.actingBlockLength = MessageHeader.blockLength(buffer, offset);
        return (T)this;
    }

    /**
     * Total encoded length including header (and repeating groups where present).
     *
     * @return encoded length in bytes.
     */
    public int encodedLength()
    {
        return MessageHeader.LENGTH + blockLength();
    }

    /**
     * Buffer currently wrapped.
     *
     * @return the wrapped buffer.
     */
    public DirectBuffer buffer()
    {
        return buffer;
    }

    /**
     * Offset of the message header in the wrapped buffer.
     *
     * @return message offset.
     */
    public int messageOffset()
    {
        return messageOffset;
    }

    /**
     * Read a little-endian long from the root block.
     *
     * @param fieldOffset relative to the root block.
     * @return the value.
     */
    protected final long getLong(final int fieldOffset)
    {
        return buffer.getLong(offset + fieldOffset, BYTE_ORDER);
    }

    /**
     * Write a little-endian long into the root block.
     *
     * @param fieldOffset relative to the root block.
     * @param value       to write.
     */
    protected final void putLong(final int fieldOffset, final long value)
    {
        mutableBuffer.putLong(offset + fieldOffset, value, BYTE_ORDER);
    }

    /**
     * Read a little-endian int from the root block.
     *
     * @param fieldOffset relative to the root block.
     * @return the value.
     */
    protected final int getInt(final int fieldOffset)
    {
        return buffer.getInt(offset + fieldOffset, BYTE_ORDER);
    }

    /**
     * Write a little-endian int into the root block.
     *
     * @param fieldOffset relative to the root block.
     * @param value       to write.
     */
    protected final void putInt(final int fieldOffset, final int value)
    {
        mutableBuffer.putInt(offset + fieldOffset, value, BYTE_ORDER);
    }

    /**
     * Read a byte from the root block.
     *
     * @param fieldOffset relative to the root block.
     * @return the value.
     */
    protected final byte getByte(final int fieldOffset)
    {
        return buffer.getByte(offset + fieldOffset);
    }

    /**
     * Write a byte into the root block.
     *
     * @param fieldOffset relative to the root block.
     * @param value       to write.
     */
    protected final void putByte(final int fieldOffset, final byte value)
    {
        mutableBuffer.putByte(offset + fieldOffset, value);
    }
}
