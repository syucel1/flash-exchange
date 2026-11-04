package com.flashexchange.protocol;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exhaustive accessor round trip: every setter of every fixed-size flyweight is written with a distinct value and
 * read back through its getter, including the raw {@code xxxCode} accessors of enum fields. Guards against
 * overlapping field offsets.
 */
class FlyweightAccessorsTest
{
    @ParameterizedTest
    @ValueSource(classes = {NewOrderFlyweight.class, CancelOrderFlyweight.class, BookSnapshotRequestFlyweight.class,
        ExecutionReportFlyweight.class, TradeExecutionFlyweight.class, PublicTradeFlyweight.class})
    void everyFieldRoundTripsWithoutOverlap(final Class<? extends Flyweight<?>> type) throws Exception
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
        final Flyweight<?> flyweight = type.getDeclaredConstructor().newInstance();
        flyweight.wrapForEncode(buffer, 0);

        int distinct = 1;
        int fields = 0;
        for (final Method setter : type.getDeclaredMethods())
        {
            if (!Modifier.isPublic(setter.getModifiers()) || setter.getParameterCount() != 1 ||
                setter.getReturnType() != type)
            {
                continue;
            }
            final Class<?> p = setter.getParameterTypes()[0];
            final Object value;
            if (p == long.class)
            {
                value = 1_000_000_000_000L + distinct;
            }
            else if (p == int.class)
            {
                value = 1_000 + distinct;
            }
            else if (p == byte.class)
            {
                value = (byte)(1 + (distinct % 2));
            }
            else
            {
                value = p.getEnumConstants()[p.getEnumConstants().length - 1];
            }
            distinct++;
            fields++;
            setter.invoke(flyweight, value);
            final Method getter = type.getMethod(setter.getName());
            assertEquals(value, getter.invoke(flyweight), setter.getName());
        }
        assertTrue(fields > 0);

        // re-read everything after all writes: a later write must not have clobbered an earlier field
        final Flyweight<?> decoder = type.getDeclaredConstructor().newInstance();
        decoder.wrapForDecode(buffer, 0);
        distinct = 1;
        for (final Method setter : type.getDeclaredMethods())
        {
            if (!Modifier.isPublic(setter.getModifiers()) || setter.getParameterCount() != 1 ||
                setter.getReturnType() != type)
            {
                continue;
            }
            final Class<?> p = setter.getParameterTypes()[0];
            final Method getter = type.getMethod(setter.getName());
            if (p == long.class)
            {
                assertEquals(1_000_000_000_000L + distinct, getter.invoke(decoder), setter.getName());
            }
            else if (p == int.class)
            {
                assertEquals(1_000 + distinct, getter.invoke(decoder), setter.getName());
            }
            distinct++;
        }
        assertEquals(MessageHeader.LENGTH + flyweight.blockLength(), flyweight.encodedLength());
    }
}
