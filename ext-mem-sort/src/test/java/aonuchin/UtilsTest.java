package aonuchin;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class UtilsTest {
    public static final Collection<Integer> TEST_DATA_SIZES = Arrays.asList(
            0, 1, 500, 50000, 10000000);

    @Test
    public void testSample() {
        for (int populationSize : TEST_DATA_SIZES) {
            List<Long> population = new ArrayList<>();
            for (int i = 0; i < populationSize; i++) {
                population.add(ThreadLocalRandom.current().nextLong());
            }
            assertEquals(populationSize, population.size());
            for (int sampleSize : Arrays.asList(0, Math.min(1, populationSize), Math.max(0, populationSize / 10), Math.max(0, populationSize - 1))) {
                List<Long> sample = new ArrayList<>();
                Utils.sampleAndFindMin(population, sample, sampleSize);
                assertEquals(sampleSize, sample.size());
            }
            List<Long> sample = new ArrayList<>();
            Utils.sampleAndFindMin(population, sample, populationSize);
            assertEquals(sample, population);
        }
    }


    @Test
    public void testSliceListBuffersPool() {
        List<ByteBuffer> buffersPool = Utils.buildBuffersPool((long) Integer.MAX_VALUE + 10);
        List<ByteBuffer> smallBuffers = Utils.sliceListBuffersPool(buffersPool, 25000, 99);
        assertEquals(99, smallBuffers.size());
        for (ByteBuffer smallBuffer : smallBuffers) {
            assertEquals(25000, smallBuffer.capacity());
        }
    }
}
