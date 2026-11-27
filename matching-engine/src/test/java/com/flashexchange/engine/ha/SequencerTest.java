package com.flashexchange.engine.ha;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencerTest
{
    @Test
    void assignsSequencesAndDetectsDuplicates()
    {
        final Sequencer sequencer = new Sequencer(16);
        assertEquals(-1, sequencer.lastSequence());
        assertEquals(0, sequencer.next());
        assertEquals(1, sequencer.next());
        assertEquals(1, sequencer.lastSequence());
        assertFalse(sequencer.checkDuplicate(1, 5));
        assertTrue(sequencer.checkDuplicate(1, 5));
        assertFalse(sequencer.checkDuplicate(2, 5), "client order ids are per user");
        assertFalse(sequencer.checkDuplicate(1, 0));
        assertFalse(sequencer.checkDuplicate(1, 0), "client order id 0 is never deduplicated");
        assertEquals(16, sequencer.dedupWindow().capacity());
    }

    @Test
    void dedupCanBeDisabled()
    {
        final Sequencer sequencer = new Sequencer(0);
        assertFalse(sequencer.checkDuplicate(1, 5));
        assertFalse(sequencer.checkDuplicate(1, 5));
        sequencer.observe(3, true, false, 1, 6);
        assertEquals(0, sequencer.dedupWindow().size());
    }

    @Test
    void observeFollowsAnotherSequencer()
    {
        final Sequencer sequencer = new Sequencer(16);
        sequencer.observe(4, true, false, 1, 9);
        sequencer.observe(5, true, true, 1, 9);
        sequencer.observe(6, false, false, 1, 10);
        sequencer.observe(2, false, false, 1, 11);
        assertEquals(6, sequencer.lastSequence());
        assertEquals(1, sequencer.dedupWindow().size());
        assertTrue(sequencer.checkDuplicate(1, 9));
        assertFalse(sequencer.checkDuplicate(1, 10), "cancels do not enter the window");
        assertEquals(7, sequencer.next());
    }

    @Test
    void stateRoundTrip()
    {
        final Sequencer sequencer = new Sequencer(8);
        sequencer.next();
        sequencer.checkDuplicate(3, 33);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = sequencer.write(buffer, 0);

        final Sequencer copy = new Sequencer(8);
        assertEquals(length, copy.read(buffer, 0));
        assertEquals(0, copy.lastSequence());
        assertTrue(copy.checkDuplicate(3, 33));

        final UnsafeBuffer bad = new UnsafeBuffer(new byte[16]);
        bad.putInt(0, 99);
        assertThrows(IllegalStateException.class, () -> copy.read(bad, 0));
    }
}
