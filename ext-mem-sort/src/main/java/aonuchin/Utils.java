package aonuchin;

import aonuchin.nio.ChannelIterable;
import aonuchin.nio.ElementSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Utils {

    public static <E> E readElement(ElementSerializer<E> serializer, ByteBuffer byteBuffer) {
        E e = serializer.readElement(byteBuffer, byteBuffer.position());
        byteBuffer.position(byteBuffer.position() + serializer.elementSize());
        return e;
    }

    public static <E> void writeElement(ElementSerializer<E> serializer, ByteBuffer byteBuffer, E element) {
        serializer.writeElement(byteBuffer, byteBuffer.position(), element);
        byteBuffer.position(byteBuffer.position() + serializer.elementSize());
    }
    public static <E extends Comparable<E>> E sampleAndFindMin(Iterable<E> pupulation, List<E> sample, int sampleCapacity) {
        return sampleAndFindMin(pupulation, sample, sampleCapacity,Ordering.<E>natural());
    }

    public static <E> E sampleAndFindMin(Iterable<E> pupulation, List<E> sample, int sampleCapacity, Comparator<E> comparator) {
        sample.clear();
        int i = 0;
        E min = null;
        Ordering<E> ordering = Ordering.from(comparator);
        for (E element : pupulation) {
            if (min == null) {
                min = element;
            }
            min = ordering.min(min, element);
            if (i < sampleCapacity) {
                sample.add(element);
            } else {
                int j = ThreadLocalRandom.current().nextInt(i + 1);
                if (j < sampleCapacity) {
                    sample.set(j, element);
                }
            }
            i++;
        }
        Preconditions.checkArgument(i >= sampleCapacity, i + " " + sampleCapacity);
        return min;
    }

    public static <E extends Comparable<E>> List<ByteBuffer> sliceListBuffersPool(List<ByteBuffer> buffersPool, int smallBufferSize, int buffersCount) {
        List<ByteBuffer> smallBuffers = new ArrayList<>();
        for (ByteBuffer bigBuffer : buffersPool) {
            bigBuffer.rewind();

            while (bigBuffer.capacity() - bigBuffer.position() > smallBufferSize) {
                if (smallBuffers.size() == buffersCount) {
                    return smallBuffers;
                }
                bigBuffer.limit(bigBuffer.position() + smallBufferSize);
                smallBuffers.add(bigBuffer.slice());
                bigBuffer.position(bigBuffer.limit());
            }
            if (smallBuffers.size() == buffersCount) {
                return smallBuffers;
            }
        }
        throw new IndexOutOfBoundsException( " " + smallBuffers.size() + " " + smallBufferSize);
    }

    public static List<ByteBuffer> buildBuffersPool(long capacity) {
        List<ByteBuffer> byteBuffersPool = new ArrayList<>();
        int n = (int) (capacity / Integer.MAX_VALUE);
        if (capacity % Integer.MAX_VALUE != 0) {
            n++;
        }
        int bufferSize = (int) (capacity / n);
        for (int i = 0; i < n; i++) {
            byteBuffersPool.add(ByteBuffer.allocateDirect(bufferSize));
        }
        return byteBuffersPool;
    }
}
