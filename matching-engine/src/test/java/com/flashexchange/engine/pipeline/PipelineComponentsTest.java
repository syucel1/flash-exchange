package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.matching.MatchingEngine;
import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineComponentsTest
{
    @Test
    void messageBatchAppendsDrainsAndClears()
    {
        final MessageBatch batch = new MessageBatch(4);
        batch.buffer().putLong(batch.limit(), 11L);
        batch.commit(8);
        batch.buffer().putInt(batch.limit(), 22);
        batch.commit(4);
        assertEquals(2, batch.count());
        assertEquals(8, batch.length(0));
        assertEquals(4, batch.length(1));
        assertEquals(12, batch.limit());

        final List<Long> seen = new ArrayList<>();
        final int accepted = batch.drainTo((buffer, offset, length) ->
        {
            seen.add(length == 8 ? buffer.getLong(offset) : buffer.getInt(offset));
            return length == 8;
        });
        assertEquals(1, accepted);
        assertEquals(List.of(11L, 22L), seen);

        batch.clear();
        assertEquals(0, batch.count());
        assertEquals(0, batch.limit());
        assertThrows(IllegalArgumentException.class, () -> batch.commit(0));
    }

    static final class ScriptedTarget implements OfferingMessagePublisher.OfferTarget
    {
        final Deque<Long> results = new ArrayDeque<>();
        int calls;

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length)
        {
            calls++;
            return results.isEmpty() ? 1 : results.poll();
        }
    }

    static final class ManualClock implements org.agrona.concurrent.NanoClock
    {
        long now = 1_000;

        @Override
        public long nanoTime()
        {
            return now;
        }
    }

    @Test
    void offeringPublisherRetriesBackPressureThenSucceeds()
    {
        final ScriptedTarget target = new ScriptedTarget();
        target.results.addAll(List.of(Publication.BACK_PRESSURED, Publication.ADMIN_ACTION, 42L));
        final int[] idles = new int[1];
        final OfferingMessagePublisher publisher = new OfferingMessagePublisher("test", target,
            countingIdle(idles), new ManualClock(), 1_000);

        assertTrue(publisher.publish(new UnsafeBuffer(new byte[8]), 0, 8));
        assertEquals(3, target.calls);
        assertEquals(2, idles[0]);
        assertEquals(1, publisher.published());
        assertEquals("test", publisher.name());
    }

    @Test
    void offeringPublisherDropsAfterTimeoutAndOnFailures()
    {
        final ScriptedTarget target = new ScriptedTarget();
        final ManualClock clock = new ManualClock();
        final int[] idles = new int[1];
        final OfferingMessagePublisher publisher = new OfferingMessagePublisher("p", target,
            new IdleStrategy()
            {
                @Override
                public void idle(final int workCount)
                {
                }

                @Override
                public void idle()
                {
                    idles[0]++;
                    clock.now += 600;
                }

                @Override
                public void reset()
                {
                }
            }, clock, 1_000);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        for (int i = 0; i < 10; i++)
        {
            target.results.add(Publication.BACK_PRESSURED);
        }
        assertFalse(publisher.publish(buffer, 0, 8));
        assertEquals(1, publisher.droppedBackPressured());

        target.results.clear();
        target.results.add(Publication.NOT_CONNECTED);
        assertFalse(publisher.publish(buffer, 0, 8));
        assertEquals(1, publisher.droppedNotConnected());

        target.results.add(Publication.CLOSED);
        assertFalse(publisher.publish(buffer, 0, 8));
        target.results.add(Publication.MAX_POSITION_EXCEEDED);
        assertFalse(publisher.publish(buffer, 0, 8));
        assertEquals(2, publisher.failed());
        assertEquals(0, publisher.published());
        assertTrue(publisher.toString().contains("droppedNotConnected=1"));
    }

    private static IdleStrategy countingIdle(final int[] counter)
    {
        return new IdleStrategy()
        {
            @Override
            public void idle(final int workCount)
            {
            }

            @Override
            public void idle()
            {
                counter[0]++;
            }

            @Override
            public void reset()
            {
            }
        };
    }

    @Test
    void orderCommandStampsOrderIdsOnlyForNewOrders()
    {
        final OrderCommand command = new OrderCommand();
        command.newOrder(1, 2, 3, (byte)1, (byte)1, (byte)1, (byte)0, true, 100, 10).stamp(41, 999);
        assertEquals(CommandType.NEW_ORDER, command.type());
        assertEquals(42, command.orderId());
        assertEquals(41, command.engineSequence());
        assertEquals(999, command.timestamp());
        assertEquals(3, command.instrumentId());
        assertTrue(command.postOnly);

        command.cancelOrder(5, 2, 3, 77).stamp(50, 1);
        assertEquals(CommandType.CANCEL_ORDER, command.type());
        assertEquals(77, command.orderId(), "cancel keeps its target order id");

        command.bookSnapshot(9).stamp(51, 2);
        assertEquals(CommandType.BOOK_SNAPSHOT, command.type());
        assertEquals(9, command.instrumentId());

        command.executionOutput().commit(4);
        command.marketDataOutput().commit(4);
        command.clearOutput();
        assertEquals(0, command.executionOutput().count());
        assertEquals(0, command.marketDataOutput().count());
    }

    @Test
    void matcherHandlerSkipsOtherShardsAndRoutesCommands()
    {
        final EncodingEventSink sink = new EncodingEventSink();
        final MatchingEngine engine = new MatchingEngine(
            List.of(new Instrument(2, "A", 1, 1, 1, 100, 0)), sink, 4, 0);
        final MatcherEventHandler handler = new MatcherEventHandler(0, 2, engine, sink);

        final OrderCommand foreign = new OrderCommand().newOrder(1, 1, 3, (byte)1, (byte)1, (byte)1, (byte)0,
            false, 10, 1);
        handler.onEvent(foreign.stamp(0, 0), 0, true);
        assertEquals(0, foreign.executionOutput().count(), "instrument 3 belongs to shard 1");

        final OrderCommand own = new OrderCommand().newOrder(1, 1, 2, (byte)1, (byte)1, (byte)1, (byte)0,
            false, 10, 1);
        handler.onEvent(own.stamp(1, 0), 1, true);
        assertEquals(1, own.executionOutput().count());
        assertEquals(1, own.marketDataOutput().count());

        final OrderCommand cancel = new OrderCommand().cancelOrder(9, 1, 2, 2);
        handler.onEvent(cancel.stamp(2, 0), 2, true);
        assertEquals(1, cancel.executionOutput().count());

        final OrderCommand snapshot = new OrderCommand().bookSnapshot(2);
        handler.onEvent(snapshot.stamp(3, 0), 3, true);
        assertEquals(1, snapshot.marketDataOutput().count());

        final OrderCommand broken = new OrderCommand().bookSnapshot(2);
        broken.type = null;
        assertThrows(NullPointerException.class, () -> handler.onEvent(broken, 4, true));

        assertEquals(1, MatcherEventHandler.shardOf(-3, 2));
        assertEquals(0, MatcherEventHandler.shardOf(4, 2));
        assertThrows(IllegalArgumentException.class, () -> new MatcherEventHandler(2, 2, engine, sink));
        assertThrows(IllegalArgumentException.class, () -> new MatcherEventHandler(-1, 2, engine, sink));
        assertThrows(IllegalArgumentException.class, () -> new MatcherEventHandler(0, 0, engine, sink));
    }

    @Test
    void publisherHandlerDrainsAndClearsEvenOnFailure()
    {
        final CapturingPublisher exec = new CapturingPublisher();
        final PublisherEventHandler handler = new PublisherEventHandler(exec, (b, o, l) ->
        {
            throw new IllegalStateException("boom");
        });
        final OrderCommand command = new OrderCommand();
        command.executionOutput().commit(8);
        command.marketDataOutput().commit(8);
        assertEquals(-1, handler.lastPublishedSequence());
        assertThrows(IllegalStateException.class, () -> handler.onEvent(command, 5, true));
        assertEquals(1, exec.size());
        assertEquals(0, command.executionOutput().count());
        assertEquals(5, handler.lastPublishedSequence());
    }

    @Test
    void exceptionHandlerCountsAndContinues()
    {
        final CountingExceptionHandler handler = new CountingExceptionHandler();
        handler.handleEventException(new RuntimeException("x"), 1, new OrderCommand());
        handler.handleEventException(new RuntimeException("y"), 2, null);
        handler.handleOnStartException(new RuntimeException("start"));
        handler.handleOnShutdownException(new RuntimeException("stop"));
        assertEquals(2, handler.eventExceptions());
    }
}
