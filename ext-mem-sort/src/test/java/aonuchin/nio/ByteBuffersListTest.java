package aonuchin.nio;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Pipe;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class ByteBuffersListTest {

    @Test
    public void testRandomLongsSort() throws Exception {
        for (int i = 1; i < 1000; i++) {
            List<Long> expected = new ArrayList<>(i);
            ByteBuffer bb = ByteBuffer.allocateDirect(i * 8);
            for (int j = 0; j < i; j++) {
                expected.add(ThreadLocalRandom.current().nextLong());
                bb.putLong(expected.get(j));
            }
            checkSortBuffersList(i, expected, bb);
        }
    }

    @Test
    public void testSingleValueLongsSort() throws Exception {
        for (int i = 1; i < 1000; i++) {
            List<Long> expected = new ArrayList<>(i);
            ByteBuffer bb = ByteBuffer.allocateDirect(i * 8);
            for (int j = 0; j < i; j++) {
                expected.add(0L);
                bb.putLong(expected.get(j));
            }
            checkSortBuffersList(i, expected, bb);
        }
    }

    @Test
    public void testTwoValuesLongSort() throws Exception {
        for (int i = 1; i < 1000; i++) {
            List<Long> expected = new ArrayList<>(i);
            ByteBuffer bb = ByteBuffer.allocateDirect(i * 8);
            for (int j = 0; j < i; j++) {
                expected.add(j % 2 == 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
                bb.putLong(expected.get(j));
            }
            checkSortBuffersList(i, expected, bb);
        }
    }

    @Test
    public void testEmptyLongSort() throws Exception {
        List<Long> expected = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.allocateDirect(0);
        checkSortBuffersList(0, expected, bb);
    }

    private void checkSortBuffersList(int i, List<Long> expected, ByteBuffer bb) throws Exception {
        Collections.sort(expected);
        ByteBuffersList<Long> list = createListAndReadFromChannel(bb);
        Collections.sort(list);
        assertEquals(list.capacity() * 8, list.capacityInBytes());
        assertEquals(expected, list);
    }

    private ByteBuffersList<Long> createListAndReadFromChannel(final ByteBuffer bb) throws Exception {
        List<ByteBuffer> buffersPool = Arrays.asList(
                ByteBuffer.allocateDirect(bb.capacity() / 2 + 9),
                ByteBuffer.allocateDirect(bb.capacity() / 2 + + 9));
        ByteBuffersList<Long> longs = new ByteBuffersList<>(buffersPool, new LongSerializer());
        Path tempFile = Files.createTempFile(null, null);
        try (ByteChannel channel = Files.newByteChannel(tempFile, EnumSet.of(StandardOpenOption.WRITE))) {
            bb.rewind();
            bb.limit(bb.capacity());
            while (bb.hasRemaining()) {
                channel.write(bb);
            }
        }
        try (ByteChannel channel = Files.newByteChannel(tempFile, EnumSet.of(StandardOpenOption.READ))) {
            longs.readFromChannel(channel);
        }
        Files.delete(tempFile);
        return longs;
    }
}
