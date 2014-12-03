package aonuchin.nio;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

public class ByteBuffersList<E> extends AbstractList<E> implements RandomAccess {


    private static class Address {
        int bufferNo;
        int offset;

        private Address(int bufferNo, int offset) {
            this.bufferNo = bufferNo;
            this.offset = offset;
        }
    }
    private final List<ByteBuffer> buffersPool;
    private final ElementSerializer<E> serializer;
    private final int capacity;
    private int size;


    public ByteBuffersList(List<ByteBuffer> buffersPool, ElementSerializer<E> serializer) {
        this.buffersPool = buffersPool;
        this.serializer = serializer;
        int capacity = 0;
        for (ByteBuffer buffer : buffersPool) {
            capacity += buffer.capacity() / serializer.elementSize();
        }
        this.capacity = capacity;
        clear();
    }

    @Override
    public void clear() {
        size = 0;
        for (ByteBuffer bb : buffersPool) {
            bb.limit(align(bb.capacity()));
        }
    }

    @Override
    public void add(int index, E element) {
        Preconditions.checkArgument(index == size, "This version of list allows only adding in the end! " +
                index + " " + size + " " + capacity);
        checkIndexBounds(index < capacity, index);
        Address address = addressOfIndex(index);
        serializer.writeElement(buffersPool.get(address.bufferNo), address.offset, element);
        size++;
    }

    private void checkIndexBounds(boolean boundsCheck, int index) {
        if (!boundsCheck) {
            throw new IndexOutOfBoundsException("Index: " + index + " size: " + size + " capacity: " + capacity);
        }
    }

    @Override
    public E remove(int index) {
        Preconditions.checkArgument(index == size - 1, "This version of list allows only adding in the end! " +
                index + " " + size + " " + capacity);
        checkIndexBounds(!isEmpty(), index);
        E e = get(index);
        size--;
        return e;
    }

    @Override
    public E set(int index, E element) {
        E e = get(index);
        Address address = addressOfIndex(index);
        serializer.writeElement(buffersPool.get(address.bufferNo), address.offset, element);
        return e;
    }

    @Override
    public E get(int index) {
        checkIndexBounds((0 <= index) && (index < size), index);
        Address address = addressOfIndex(index);
        return serializer.readElement(buffersPool.get(address.bufferNo), address.offset);
    }

    @Override
    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public long capacityInBytes() {
        return capacity * (long) serializer.elementSize();
    }

    private Address addressOfIndex(int index) {
        long position = index * (long) serializer.elementSize();
        for (int bufferNo = 0; bufferNo < buffersPool.size(); bufferNo++) {
            int alignedBufferSize = align(buffersPool.get(bufferNo).capacity());
            if (position < alignedBufferSize) {
                return new Address(bufferNo, (int) position);
            }
            position -= alignedBufferSize;
        }
        throw new IndexOutOfBoundsException();
    }

    private int align(int capacity) {
        return capacity - (capacity % serializer.elementSize());
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException {
        // asserting that channel is blocking
        Address endAddress = addressOfIndex(size);
        for (int bufferNo = 0; bufferNo < endAddress.bufferNo; bufferNo++) {
            ByteBuffer buffer = buffersPool.get(bufferNo);
            buffer.limit(align(buffer.capacity()));
            buffer.rewind();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
        ByteBuffer buffer = buffersPool.get(endAddress.bufferNo);
        buffer.limit(endAddress.offset);
        buffer.rewind();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    public void readFromChannel(ReadableByteChannel channel) throws IOException {
        // asserting that channel is blocking
        clear();
        long totalReadBytes = 0;
        int readBytes = 0;
        for (ByteBuffer buffer : buffersPool) {
            buffer.limit(align(buffer.capacity()));
            buffer.rewind();
            do {
                readBytes = channel.read(buffer);
                if (readBytes < 0) {
                    size = (int) (totalReadBytes / serializer.elementSize());
                    return;
                } else {
                    totalReadBytes += readBytes;
                }
            } while (readBytes > 0);
            Preconditions.checkArgument(totalReadBytes % serializer.elementSize() == 0);
            Preconditions.checkArgument(buffer.position() == align(buffer.capacity()));
        }
        size = (int) (totalReadBytes / serializer.elementSize());
        Preconditions.checkArgument(readBytes < 0);
    }

    public ElementSerializer<E> getSerializer() {
        return serializer;
    }

    public List<ByteBuffer> getBuffersPool() {
        return buffersPool;
    }
}
