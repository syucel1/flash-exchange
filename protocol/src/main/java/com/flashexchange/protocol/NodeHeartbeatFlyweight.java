package com.flashexchange.protocol;

/**
 * Engine node to arbiter and peer: liveness and replication progress.
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  epoch
 *      8  8  lastSequence
 *     16  8  releasedSequence
 *     24  8  timestamp
 *     32  4  nodeId
 *     36  1  role
 *     37  1  caughtUp
 *     38  1  peerVisible
 * </pre>
 */
public final class NodeHeartbeatFlyweight extends Flyweight<NodeHeartbeatFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 52;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 48;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int EPOCH_OFFSET = 0;
    private static final int LAST_SEQUENCE_OFFSET = 8;
    private static final int RELEASED_SEQUENCE_OFFSET = 16;
    private static final int TIMESTAMP_OFFSET = 24;
    private static final int NODE_ID_OFFSET = 32;
    private static final int ROLE_OFFSET = 36;
    private static final int CAUGHT_UP_OFFSET = 37;
    private static final int PEER_VISIBLE_OFFSET = 38;

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
     * Epoch the node is in.
     *
     * @return the value.
     */
    public long epoch()
    {
        return getLong(EPOCH_OFFSET);
    }

    /**
     * Epoch the node is in.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight epoch(final long value)
    {
        putLong(EPOCH_OFFSET, value);
        return this;
    }

    /**
     * Highest engine sequence the node has sequenced (primary) or applied (standby).
     *
     * @return the value.
     */
    public long lastSequence()
    {
        return getLong(LAST_SEQUENCE_OFFSET);
    }

    /**
     * Highest engine sequence the node has sequenced (primary) or applied (standby).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight lastSequence(final long value)
    {
        putLong(LAST_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Highest engine sequence whose output the node has released to clients.
     *
     * @return the value.
     */
    public long releasedSequence()
    {
        return getLong(RELEASED_SEQUENCE_OFFSET);
    }

    /**
     * Highest engine sequence whose output the node has released to clients.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight releasedSequence(final long value)
    {
        putLong(RELEASED_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Sender clock (epoch milliseconds).
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Sender clock (epoch milliseconds).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return this;
    }

    /**
     * Sender node id.
     *
     * @return the value.
     */
    public int nodeId()
    {
        return getInt(NODE_ID_OFFSET);
    }

    /**
     * Sender node id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight nodeId(final int value)
    {
        putInt(NODE_ID_OFFSET, value);
        return this;
    }

    /**
     * Current role.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public NodeRole role()
    {
        return NodeRole.get(getByte(ROLE_OFFSET));
    }

    /**
     * Raw wire code of {@link #role()}.
     *
     * @return the encoded byte.
     */
    public byte roleCode()
    {
        return getByte(ROLE_OFFSET);
    }

    /**
     * Current role.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight role(final NodeRole value)
    {
        putByte(ROLE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #role()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight roleCode(final byte code)
    {
        putByte(ROLE_OFFSET, code);
        return this;
    }

    /**
     * 1 when a standby is live on the replication stream (0 while bootstrapping).
     *
     * @return the value.
     */
    public byte caughtUp()
    {
        return getByte(CAUGHT_UP_OFFSET);
    }

    /**
     * 1 when a standby is live on the replication stream (0 while bootstrapping).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight caughtUp(final byte value)
    {
        putByte(CAUGHT_UP_OFFSET, value);
        return this;
    }

    /**
     * 1 when the node currently receives heartbeats from its peer.
     *
     * @return the value.
     */
    public byte peerVisible()
    {
        return getByte(PEER_VISIBLE_OFFSET);
    }

    /**
     * 1 when the node currently receives heartbeats from its peer.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NodeHeartbeatFlyweight peerVisible(final byte value)
    {
        putByte(PEER_VISIBLE_OFFSET, value);
        return this;
    }
}
