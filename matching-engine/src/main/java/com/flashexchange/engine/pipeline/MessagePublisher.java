package com.flashexchange.engine.pipeline;

import org.agrona.DirectBuffer;

/**
 * Destination for encoded engine output (an Aeron publication in production).
 */
@FunctionalInterface
public interface MessagePublisher
{
    /**
     * Publish one message.
     *
     * @param buffer containing the message.
     * @param offset of the message.
     * @param length of the message.
     * @return true if the message was handed to the transport, false if it was dropped.
     */
    boolean publish(DirectBuffer buffer, int offset, int length);
}
