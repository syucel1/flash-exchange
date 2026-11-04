package com.flashexchange.engine.pipeline;

import com.flashexchange.protocol.EngineEventListener;
import com.flashexchange.protocol.EngineMessageDispatcher;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-safe test publisher that keeps a copy of every message.
 */
public final class CapturingPublisher implements MessagePublisher
{
    private final List<byte[]> messages = new ArrayList<>();

    @Override
    public synchronized boolean publish(final DirectBuffer buffer, final int offset, final int length)
    {
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        messages.add(copy);
        return true;
    }

    public synchronized int size()
    {
        return messages.size();
    }

    public synchronized List<byte[]> messages()
    {
        return new ArrayList<>(messages);
    }

    public void replay(final EngineEventListener listener)
    {
        final EngineMessageDispatcher dispatcher = new EngineMessageDispatcher();
        for (final byte[] message : messages())
        {
            dispatcher.dispatch(new UnsafeBuffer(message), 0, message.length, listener);
        }
    }
}
