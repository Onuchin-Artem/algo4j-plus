package me.aonuchin.orderstat;

import com.google.common.collect.Ordering;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class MunroPatersonTest {

    private static final int[] TEST_SIZES = new int[]{1, 10, 1000, 1000000};
    private static final int[] TEST_STEPS = new int[]{1, 10, 10, 10000};

    @Test
    public void testFindKOrderStatisticsUniqueElements() throws Exception {
        for (int caseNo = 0; caseNo < TEST_SIZES.length; caseNo++) {
            int testSize = TEST_SIZES[caseNo];
            int testStep = TEST_STEPS[caseNo];
            ArrayList<Long> randomList = new ArrayList<>();
            for (int i = 0; i < testSize; i++) {
                randomList.add(Long.valueOf(i));
            }
            Collections.shuffle(randomList, new Random(42));
            testOrderStatistics(randomList, testStep);
        }
    }

    private void testOrderStatistics(ArrayList<Long> randomList, int testStep) throws Exception {
        ArrayList<Long> sortedList = new ArrayList<>(randomList);
        Collections.sort(sortedList);

        DelegatingElementStreamFactory<Long> streamFactory = new DelegatingElementStreamFactory<>(randomList);
        MunroPaterson<Long> searcher = MunroPaterson.create();
        for (int kOrder = 0; kOrder < randomList.size(); kOrder += testStep) {
            streamFactory.resetPasses();
            assertEquals(sortedList.get(kOrder), searcher.findKOrderStatistics(streamFactory, kOrder));
        }
        assertEquals(sortedList.get(randomList.size() - 1), searcher.findKOrderStatistics(streamFactory, randomList.size() - 1));
    }

    @Test
    public void testFindKOrderStatistics2Elements() throws Exception {
            int testSize = 100000;
            int testStep = 1000;
            ArrayList<Long> randomList = new ArrayList<>();
            for (int i = 0; i < testSize; i++) {
                randomList.add(Long.valueOf(i % 2));
            }
            Collections.shuffle(randomList, new Random(42));
            testOrderStatistics(randomList, testStep);
    }

    @Test
    public void testFindKOrderStatisticsSameElements() throws Exception {
        int testSize = 100000;
        int testStep = 1000;
        ArrayList<Long> randomList = new ArrayList<>();
        for (int i = 0; i < testSize; i++) {
            randomList.add(0L);
        }
        testOrderStatistics(randomList, testStep);
    }

    @Test
    public void testFindKOrderStatisticsAscElements() throws Exception {
        int testSize = 100000;
        int testStep = 1000;
        ArrayList<Long> randomList = new ArrayList<>();
        for (int i = 0; i < testSize; i++) {
            randomList.add(ThreadLocalRandom.current().nextLong(10000));
        }
        Collections.sort(randomList);
        testOrderStatistics(randomList, testStep);
    }

    @Test
    public void testFindKOrderStatisticsDescElements() throws Exception {
        int testSize = 100000;
        int testStep = 1000;
        ArrayList<Long> randomList = new ArrayList<>();
        for (int i = 0; i < testSize; i++) {
            randomList.add(ThreadLocalRandom.current().nextLong(10000));
        }
        Collections.sort(randomList, Ordering.natural().reverse());
        testOrderStatistics(randomList, testStep);
    }

    @Test
    public void testPowerOfTwo() {
        assertEquals(1L, MunroPaterson.powerOfTwo(0));
        assertEquals(2L, MunroPaterson.powerOfTwo(1));
        assertEquals(4L, MunroPaterson.powerOfTwo(2));
        assertEquals(8L, MunroPaterson.powerOfTwo(3));
        assertEquals(576460752303423488L, MunroPaterson.powerOfTwo(59));
    }
}