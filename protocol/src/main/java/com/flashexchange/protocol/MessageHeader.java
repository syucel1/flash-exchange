package com.flashexchange.protocol;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * SBE-style message header that prefixes every Flash Exchange message on Aeron.
 * <pre>
 *   0      2      4      6      8
 *   +------+------+------+------+
 *   |block |templ |schema|versn |
 *   +------+------+------+------+
 * </pre>
 * All fields are unsigned 16-bit little-endian integers. {@code blockLength} is the length of the fixed-size root
 * block that follows the header; repeating groups (if any) follow the root block.
 */
public final class MessageHeader
{
    /** Encoded length of the header in bytes. */
    public static final int LENGTH = 8;

    /** Identifier of the Flash Exchange schema. */
    public static final int SCHEMA_ID = 7001;

    /** Current schema version. */
    public static final int SCHEMA_VERSION = 1;

    /** Byte order used for every field of every message. */
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    static final int BLOCK_LENGTH_OFFSET = 0;
    static final int TEMPLATE_ID_OFFSET = 2;
    static final int SCHEMA_ID_OFFSET = 4;
    static final int VERSION_OFFSET = 6;

    private MessageHeader()
    {
    }

    /**
     * Write a header for the current schema.
     *
     * @param buffer      to write to.
     * @param offset      of the header.
     * @param blockLength of the root block that follows.
     * @param templateId  of the message.
     */
    public static void write(
        final MutableDirectBuffer buffer, final int offset, final int blockLength, final int templateId)
    {
        buffer.putShort(offset + BLOCK_LENGTH_OFFSET, (short)blockLength, BYTE_ORDER);
        buffer.putShort(offset + TEMPLATE_ID_OFFSET, (short)templateId, BYTE_ORDER);
        buffer.putShort(offset + SCHEMA_ID_OFFSET, (short)SCHEMA_ID, BYTE_ORDER);
        buffer.putShort(offset + VERSION_OFFSET, (short)SCHEMA_VERSION, BYTE_ORDER);
    }

    /**
     * Read the block length.
     *
     * @param buffer containing the header.
     * @param offset of the header.
     * @return block length.
     */
    public static int blockLength(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + BLOCK_LENGTH_OFFSET, BYTE_ORDER) & 0xFFFF;
    }

    /**
     * Read the template id.
     *
     * @param buffer containing the header.
     * @param offset of the header.
     * @return template id.
     */
    public static int templateId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + TEMPLATE_ID_OFFSET, BYTE_ORDER) & 0xFFFF;
    }

    /**
     * Read the schema id.
     *
     * @param buffer containing the header.
     * @param offset of the header.
     * @return schema id.
     */
    public static int schemaId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + SCHEMA_ID_OFFSET, BYTE_ORDER) & 0xFFFF;
    }

    /**
     * Read the schema version.
     *
     * @param buffer containing the header.
     * @param offset of the header.
     * @return schema version.
     */
    public static int version(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + VERSION_OFFSET, BYTE_ORDER) & 0xFFFF;
    }

    /**
     * Check that a buffer region holds a complete header of this schema followed by at least
     * {@code expectedBlockLength} bytes.
     *
     * @param buffer              containing the message.
     * @param offset              of the message.
     * @param length              of the message.
     * @param expectedBlockLength minimum block length the decoder needs.
     * @return true if the message can safely be decoded.
     */
    public static boolean isValid(
        final DirectBuffer buffer, final int offset, final int length, final int expectedBlockLength)
    {
        return length >= LENGTH &&
            schemaId(buffer, offset) == SCHEMA_ID &&
            blockLength(buffer, offset) >= expectedBlockLength &&
            length >= LENGTH + blockLength(buffer, offset);
    }
}
