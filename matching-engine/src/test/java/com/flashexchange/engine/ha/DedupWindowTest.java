package com.flashexchange.engine.ha;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DedupWindowTest
{
    @Test
    void detectsDuplicatesAndEvictsOldest()
    {
        final DedupWindow window = new DedupWindow(2);
        assertEquals(2, window.capacity());
        assertTrue(window.add(1, 10));
        assertFalse(window.add(1, 10));
        assertTrue(window.add(2, 10));
        assertTrue(window.contains(1, 10));
        assertTrue(window.add(3, 10));
        assertFalse(window.contains(1, 10), "oldest evicted");
        assertTrue(window.contains(2, 10));
        assertEquals(2, window.size());
        window.clear();
        assertEquals(0, window.size());
        assertFalse(window.contains(2, 10));
        assertThrows(IllegalArgumentException.class, () -> new DedupWindow(0));
    }

    @Test
    void matchesReferenceModelUnderRandomLoad()
    {
        final int capacity = 50;
        final DedupWindow window = new DedupWindow(capacity);
        final ArrayDeque<Long> fifo = new ArrayDeque<>();
        final Set<Long> set = new HashSet<>();
        final Random random = new Random(7);
        for (int i = 0; i < 100_000; i++)
        {
            final long user = random.nextInt(4);
            final long id = random.nextInt(120);
            final long key = user * 1000 + id;
            final boolean expectedNew = !set.contains(key);
            assertEquals(expectedNew, window.add(user, id), "op " + i);
            if (expectedNew)
            {
                fifo.addLast(key);
                set.add(key);
                if (fifo.size() > capacity)
                {
                    set.remove(fifo.removeFirst());
                }
            }
            assertEquals(set.size(), window.size());
        }
        for (long user = 0; user < 4; user++)
        {
            for (long id = 0; id < 120; id++)
            {
                assertEquals(set.contains(user * 1000 + id), window.contains(user, id));
            }
        }
    }

    @Test
    void serialisesInInsertionOrder()
    {
        final DedupWindow window = new DedupWindow(3);
        for (int i = 0; i < 5; i++)
        {
            window.add(i, i * 2L);
        }
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int written = window.write(buffer, 4);
        final DedupWindow copy = new DedupWindow(3);
        copy.add(99, 99);
        assertEquals(written, copy.read(buffer, 4));
        assertEquals(3, copy.size());
        assertFalse(copy.contains(99, 99));
        assertTrue(copy.contains(4, 8));
        assertTrue(copy.add(5, 10));
        assertFalse(copy.contains(2, 4), "eviction order preserved");
        assertTrue(copy.contains(3, 6));

        final DedupWindow smaller = new DedupWindow(2);
        smaller.read(buffer, 4);
        assertEquals(2, smaller.size());
        assertTrue(smaller.contains(4, 8));
        assertFalse(smaller.contains(2, 4));
    }
}
