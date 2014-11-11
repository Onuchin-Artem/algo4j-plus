package aonuchin.nio;

import aonuchin.Utils;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

public class ChannelIterable<E> implements Iterable<E>, Closeable {
    public static class Builder<E> {
        private final ByteBuffer buffer;
        private final ElementSerializer<E> serializer;

        public Builder(ByteBuffer buffer, ElementSerializer<E> serializer) {
            this.buffer = buffer;
            this.serializer = serializer;
        }

        public ChannelIterable<E> iterateOverChannel(ReadableByteChannel channel) {
            return new ChannelIterable<>(channel, buffer, serializer);
        }

        public int bufferSize() {
            return buffer.capacity();
        }


        public ByteBuffer getBuffer() {
            return buffer;
        }
    }
    private final ReadableByteChannel channel;
    private final ByteBuffer buffer;
    private final ElementSerializer<E> serializer;

    public ChannelIterable(ReadableByteChannel channel, ByteBuffer buffer, ElementSerializer<E> serializer) {
        this.channel = channel;
        this.serializer = serializer;
        this.buffer = buffer;
        setMaxLimit();
        buffer.position(buffer.limit());
    }

    private void setMaxLimit() {
        buffer.limit(buffer.capacity() - (buffer.capacity() % serializer.elementSize()));
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public Iterator<E> iterator() {
        return new AbstractIterator<E>() {
            private boolean endOfChannel = false;

            @Override
            protected E computeNext() {
                if (!buffer.hasRemaining()) {
                    if (endOfChannel) {
                        return endOfData();
                    }
                    buffer.rewind();
                    setMaxLimit();
                    int totalReadBytes = 0;
                    int readBytes;
                    do {
                        try {
                            readBytes = channel.read(buffer);
                            if (readBytes < 0) {
                                endOfChannel = true;
                            } else {
                                totalReadBytes += readBytes;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } while (readBytes > 0);
                    buffer.rewind();
                    buffer.limit(totalReadBytes);
                    Preconditions.checkArgument(totalReadBytes % serializer.elementSize() == 0,
                            totalReadBytes + " " + serializer.elementSize());
                }
                if (!buffer.hasRemaining()) {
                    if (endOfChannel) {
                        return endOfData();
                    }
                }
                return Utils.readElement(serializer, buffer);

            }
        };
    }
}
