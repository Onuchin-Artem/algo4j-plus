package me.aonucin.search;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class VanEmdeBoasLayoutTest {

    private VanEmdeBoasLayout layout;

    @Before
    public void setup() {
        layout = new VanEmdeBoasLayout(2);
    }

    @Test
    public void testRandomPointers() {
        for (int i = 0; i < 10000; i++) {
            int first = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
            int second = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
            int pos = i % 2;
            layout.setLeftChildPointer(pos, first);
            layout.setRightChildPointer(pos, second);
            assertEquals(first, layout.getLeftChildPointer(pos));
            assertEquals(second, layout.getRightChildPointer(pos));

            layout.setRightChildPointer(pos, first);
            layout.setLeftChildPointer(pos, second);
            assertEquals(second, layout.getLeftChildPointer(pos));
            assertEquals(first, layout.getRightChildPointer(pos));
        }
    }

    @Test
    public void testSetMinusOnePointers() {
        layout.setLeftChildPointer(0, -1);
        layout.setRightChildPointer(0, -1);
        assertEquals(-1, layout.getLeftChildPointer(0));
        assertEquals(-1, layout.getRightChildPointer(0));
    }

    @Test
    public void testSetZeroPointers() {
        layout.setLeftChildPointer(0, 0);
        layout.setRightChildPointer(0, 0);
        assertEquals(0, layout.getLeftChildPointer(0));
        assertEquals(0, layout.getRightChildPointer(0));
    }

    @Test
    public void testSetExtremesPointers() {
        layout.setLeftChildPointer(0, Integer.MAX_VALUE);
        layout.setRightChildPointer(0, Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, layout.getLeftChildPointer(0));
        assertEquals(Integer.MAX_VALUE, layout.getRightChildPointer(0));
    }
}