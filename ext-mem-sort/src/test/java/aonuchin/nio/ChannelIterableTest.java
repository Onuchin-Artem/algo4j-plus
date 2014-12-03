package aonuchin.nio;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class ChannelIterableTest {

    public static final Collection<Integer> TEST_DATA_SIZES = Arrays.asList(
            0, 1, 500, 50000, 10000000);

    @Test
    public void testChannelIterating() throws Exception {
         for (int i : TEST_DATA_SIZES) {
             List<ByteBuffer> buffersPool = Arrays.asList(
                     ByteBuffer.allocateDirect(i * 4 + 9),
                     ByteBuffer.allocateDirect(i * 4 + 9));
             ByteBuffersList<Long> expected = new ByteBuffersList<>(buffersPool, new LongSerializer());
             for (int j = 0; j < i; j++) {
                 expected.add(ThreadLocalRandom.current().nextLong());
             }
             Path tempFile = Files.createTempFile(null, null);
             try (ByteChannel channel = Files.newByteChannel(tempFile, EnumSet.of(StandardOpenOption.WRITE))) {
                expected.writeToChannel(channel);
             }
             List<Long> actual = new ArrayList<>(expected.size());
             try (ByteChannel channel = Files.newByteChannel(tempFile, EnumSet.of(StandardOpenOption.READ))) {
                 ChannelIterable<Long> numbersFromFile = new ChannelIterable<>(
                         channel,
                         ByteBuffer.allocateDirect(1024 * 1024),
                         new LongSerializer());
                 for (long number : numbersFromFile) {
                     actual.add(number);
                 }
             }
             Files.delete(tempFile);
             assertEquals(expected, actual);
         }
    }
}
