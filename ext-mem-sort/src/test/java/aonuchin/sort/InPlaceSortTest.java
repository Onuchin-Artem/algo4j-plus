package aonuchin.sort;

import com.google.common.collect.Ordering;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

public class InPlaceSortTest {

    public static final Collection<Integer> TEST_DATA_SIZES = Arrays.asList(
            1, 50000, 10000000);

    private ArrayList<Long> expected = new ArrayList<>(Collections.max(TEST_DATA_SIZES) * 2);
    private ArrayList<Long> actual = new ArrayList<>(Collections.max(TEST_DATA_SIZES) * 2);
    @Test
    public synchronized void testRandomLongsSort() throws Exception {
        for (int i : TEST_DATA_SIZES) {
            expected.clear();
            for (int j = 0; j < i; j++) {
                expected.add(ThreadLocalRandom.current().nextLong());
            }
            checkSort(expected);
        }
    }

    @Test
    public synchronized void testSingleValueLongsSort() throws Exception {
        for (int i : TEST_DATA_SIZES) {
            expected.clear();
            for (int j = 0; j < i; j++) {
                expected.add(0L);
            }
            checkSort(expected);
        }
    }

    @Test
    public synchronized void testTwoValuesLongSort() throws Exception {
        for (int i : TEST_DATA_SIZES) {
            expected.clear();
            for (int j = 0; j < i; j++) {
                expected.add(j % 2 == 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
            }
            checkSort(expected);
        }
    }

    @Test
    public synchronized void testSortedAscLongSort() throws Exception {
        for (int i : TEST_DATA_SIZES) {
            expected.clear();
            for (int j = 0; j < i; j++) {
                expected.add((long) j);
            }
            checkSort(expected);
        }
    }

    @Test
    public synchronized void testSortedDescLongSort() throws Exception {
        for (int i : TEST_DATA_SIZES) {
            expected.clear();
            for (int j = 0; j < i; j++) {
                expected.add((long) -j);
            }
            checkSort(expected);
        }
    }

    @Test
    public synchronized void testEmptyLongSort() throws Exception {
        expected.clear();
        checkSort(expected);
    }

    private void checkSort(List<Long> expected) throws Exception {
        actual.clear();
        actual.addAll(expected);
        Collections.sort(expected);
        InPlaceParallelSort.sort(actual, Ordering.<Long>natural());
        assertEquals(expected, actual);
    }
}
