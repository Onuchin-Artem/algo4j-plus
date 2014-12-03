package me.aonuchin.orderstat;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class MunroPatersonTest {

    private static final int[] TEST_SIZES = new int[]{1, 10, 1000};

    @Test
    public void testFindKOrderStatistics() throws Exception {
        for (int testSize : TEST_SIZES) {
            ArrayList<Long> randomList = new ArrayList<>();
            for (int i = 0; i < testSize; i++) {
                randomList.add(ThreadLocalRandom.current().nextLong());
            }
            ArrayList<Long> sortedList = new ArrayList<>(randomList);
            Collections.sort(sortedList);

            ElementsStream.Factory<Long> streamFactory = new DelegatingElementStreamFactory<>(randomList);
            MunroPaterson<Long> searcher = MunroPaterson.create();
            for (int kOrder = 0; kOrder < testSize; kOrder++) {
                assertEquals(sortedList.get(kOrder), searcher.findKOrderStatistics(streamFactory, kOrder));
            }
        }
    }
}