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
    public void testNegativePointers() {
        layout.setLeftChildPointer(0, -1);
        layout.setRightChildPointer(0, -2);
        assertEquals(-1, layout.getLeftChildPointer(0));
        assertEquals(-2, layout.getRightChildPointer(0));

        layout.setLeftChildPointer(0, -3);
        layout.setRightChildPointer(0, -2);
        assertEquals(-3, layout.getLeftChildPointer(0));
        assertEquals(-2, layout.getRightChildPointer(0));

        layout.setLeftChildPointer(0, -1);
        layout.setRightChildPointer(0, 5);
        assertEquals(-1, layout.getLeftChildPointer(0));
        assertEquals(5, layout.getRightChildPointer(0));

        layout.setLeftChildPointer(0, 5);
        layout.setRightChildPointer(0, -2);
        assertEquals(5, layout.getLeftChildPointer(0));
        assertEquals(-2, layout.getRightChildPointer(0));

    }
    private static final int[] TEST_SIZES = new int[]{1, 5, 15, 100, 10000, 100005};

    @Test
    public void testContains() {
        for (int testSize : TEST_SIZES) {
            long[] elements = new long[testSize];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = i - (i % 2);
            }
            VanEmdeBoasLayout search = VanEmdeBoasLayout.build(elements);
            for (int i = 0; i < elements.length; i++) {
                if (i % 2 == 0) {
                    assertTrue("" + i, search.contains(i));
                } else {
                    assertFalse("" + i, search.contains(i));
                }
            }
        }
    }
}