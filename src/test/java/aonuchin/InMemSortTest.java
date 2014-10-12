package aonuchin;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class InMemSortTest {
    @Test
    public void testSort() {
        for (int i = 0; i < 1000; i ++) {
            long[] expected = new long[i];
            ByteBuffer bb = ByteBuffer.allocate(i * 8);
            for (int j = 0; j < i; j++) {
                expected[j] = ThreadLocalRandom.current().nextLong();
                bb.putLong(expected[j]);
            }
            bb.rewind();
            Arrays.sort(expected);
            BufferUtils.sort(bb);
            assertEquals(i * 8, bb.limit());
            assertEquals(0, bb.position());
            long[] actual = new long[i];
            for (int j = 0; j < i; j++) {
                actual[j] = bb.getLong();
            }
            assertArrayEquals(expected, actual);
        }
    }
}
