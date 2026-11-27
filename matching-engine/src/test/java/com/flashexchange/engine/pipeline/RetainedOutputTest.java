package com.flashexchange.engine.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetainedOutputTest
{
    @Test
    void retainsTrimsCompactsAndDrainsInOrder()
    {
        final RetainedOutput retained = new RetainedOutput();
        final OrderCommand command = new OrderCommand();
        for (long seq = 0; seq < 2_000; seq++)
        {
            command.clearOutput();
            final MessageBatch exec = command.executionOutput();
            exec.buffer().putLong(exec.limit(), seq);
            exec.commit(40);
            final MessageBatch md = command.marketDataOutput();
            md.buffer().putLong(md.limit(), seq);
            md.commit(24);
            retained.retain(seq, command);
            retained.trim(seq - 10);
        }
        assertEquals(20, retained.size(), "10 sequences x 2 messages kept");

        final CapturingPublisher exec = new CapturingPublisher();
        final CapturingPublisher md = new CapturingPublisher();
        assertEquals(20, retained.drainTo(exec, md));
        assertEquals(10, exec.messages().size());
        assertEquals(10, md.messages().size());
        assertEquals(0, retained.size());

        retained.retain(5, command);
        retained.trim(5);
        assertEquals(0, retained.size());
        retained.retain(6, command);
        retained.clear();
        assertEquals(0, retained.size());
    }
}
