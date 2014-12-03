package me.aonucin.search;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class LayoutUtilTest {

    @Test
    public void testRandomLog2() throws Exception {
        for (int i = 0; i < 10000; i++) {
            int number = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
            assertEquals((int) (Math.log(number) / Math.log(2)), LayoutUtil.log2(number));
        }
    }
}