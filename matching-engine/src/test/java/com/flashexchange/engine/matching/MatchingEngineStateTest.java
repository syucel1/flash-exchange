package com.flashexchange.engine.matching;

import com.flashexchange.engine.matching.RecordingEventSink.Reject;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot determinism: an engine restored from a state snapshot must behave exactly like the engine the snapshot
 * was taken from.
 */
class MatchingEngineStateTest
{
    private static final List<Instrument> INSTRUMENTS = List.of(
        new Instrument(1, "AAA", 1, 1, 1, 1_000, 0),
        new Instrument(2, "BBB", 1, 1, 1, 1_000, 300));

    private static MatchingEngine engine(final RecordingEventSink sink)
    {
        return new MatchingEngine(INSTRUMENTS, sink, 512, 0);
    }

    /** One random command, applied identically to several engines. */
    private record Command(long seq, boolean cancel, long orderId, long user, int instrument, Side side,
        OrderType type, TimeInForce tif, SelfTradePrevention stp, boolean postOnly, long price, long quantity)
    {
        void apply(final MatchingEngine engine)
        {
            if (cancel)
            {
                engine.onCancelOrder(seq, seq * 7, 5000 + seq, user, instrument, orderId);
            }
            else
            {
                engine.onNewOrder(seq, seq * 7, seq + 1, 5000 + seq, user, instrument, side.code(), type.code(),
                    tif.code(), stp.code(), postOnly, price, quantity);
            }
        }
    }

    private static List<Command> randomCommands(final long seed, final int count)
    {
        final Random random = new Random(seed);
        final List<Command> commands = new ArrayList<>();
        for (long seq = 0; seq < count; seq++)
        {
            final long user = 1 + random.nextInt(4);
            final int instrument = 1 + random.nextInt(2);
            if (random.nextInt(5) == 0 && seq > 0)
            {
                commands.add(new Command(seq, true, 1 + random.nextInt((int)seq), user, instrument, Side.BUY,
                    OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, false, 0, 0));
                continue;
            }
            final boolean market = random.nextInt(10) == 0;
            final TimeInForce[] tifs = TimeInForce.values();
            commands.add(new Command(seq, false, 0, user, instrument, random.nextBoolean() ? Side.BUY : Side.SELL,
                market ? OrderType.MARKET : OrderType.LIMIT,
                market ? TimeInForce.IOC : tifs[random.nextInt(3)],
                SelfTradePrevention.values()[random.nextInt(SelfTradePrevention.values().length)],
                !market && random.nextInt(8) == 0,
                95 + random.nextInt(11), 1 + random.nextInt(20)));
        }
        return commands;
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 5, 77, 2024})
    void restoredEngineIsIndistinguishable(final long seed)
    {
        final List<Command> commands = randomCommands(seed, 3_000);
        final RecordingEventSink originalSink = new RecordingEventSink();
        final MatchingEngine original = engine(originalSink);
        final int cut = 1_500;
        commands.subList(0, cut).forEach(c -> c.apply(original));

        final ExpandableArrayBuffer state = new ExpandableArrayBuffer(64);
        final int length = original.writeState(state, 16);

        final RecordingEventSink restoredSink = new RecordingEventSink();
        final MatchingEngine restored = engine(restoredSink);
        assertEquals(length, restored.readState(state, 16));

        final ExpandableArrayBuffer again = new ExpandableArrayBuffer(64);
        assertEquals(length, restored.writeState(again, 0));
        for (int i = 0; i < length; i++)
        {
            assertEquals(state.getByte(16 + i), again.getByte(i), "byte " + i);
        }

        originalSink.clear();
        commands.subList(cut, commands.size()).forEach(c ->
        {
            c.apply(original);
            c.apply(restored);
        });
        assertTrue(originalSink.events.size() > 1_000);
        assertEquals(originalSink.events, restoredSink.events);
        for (final Instrument instrument : INSTRUMENTS)
        {
            assertEquals(original.book(instrument.id()).orderCount(), restored.book(instrument.id()).orderCount());
            assertEquals(original.book(instrument.id()).bookSequence(),
                restored.book(instrument.id()).bookSequence());
        }
    }

    @Test
    void rejectsIncompatibleState()
    {
        final RecordingEventSink sink = new RecordingEventSink();
        final MatchingEngine source = engine(sink);
        source.onNewOrder(0, 0, 1, 1, 1, 1, Side.BUY.code(), OrderType.LIMIT.code(), TimeInForce.GTC.code(),
            SelfTradePrevention.NONE.code(), false, 100, 5);
        final ExpandableArrayBuffer state = new ExpandableArrayBuffer(64);
        source.writeState(state, 0);

        assertThrows(IllegalStateException.class, () -> source.readState(state, 0), "non-empty");
        final MatchingEngine other = new MatchingEngine(List.of(INSTRUMENTS.get(0)), sink, 16, 0);
        assertThrows(IllegalStateException.class, () -> other.readState(state, 0), "unknown instrument");
        final MatchingEngine superset = new MatchingEngine(List.of(INSTRUMENTS.get(0), INSTRUMENTS.get(1),
            new Instrument(3, "CCC", 1, 1, 1, 10, 0)), sink, 16, 0);
        assertThrows(IllegalStateException.class, () -> superset.readState(state, 0), "instrument mismatch");

        final ExpandableArrayBuffer badVersion = new ExpandableArrayBuffer(8);
        badVersion.putInt(0, 42);
        assertThrows(IllegalStateException.class, () -> engine(sink).readState(badVersion, 0));
    }

    @Test
    void duplicateOrdersAreRejected()
    {
        final RecordingEventSink sink = new RecordingEventSink();
        engine(sink).onDuplicateOrder(3, 30, 4, 77, 9, 1, Side.SELL.code(), OrderType.LIMIT.code(),
            TimeInForce.GTC.code(), 101, 6);
        final Reject reject = sink.ofType(Reject.class).get(0);
        assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, reject.reason());
        assertEquals(77, reject.clientOrderId());
    }
}
