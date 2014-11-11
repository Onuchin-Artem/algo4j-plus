package aonuchin.nio;

import aonuchin.Utils;
import aonuchin.nio.ElementSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ChannelWriter<E> implements Closeable {
    public static class Builder {

    }
    public final ByteBuffer buffer;
    public final WritableByteChannel channel;
    private final ElementSerializer<E> serializer;

    public ChannelWriter(ElementSerializer<E> serializer, ByteBuffer buffer, WritableByteChannel channel) throws IOException {
        this.buffer = buffer;
        this.channel = channel;
        buffer.rewind();
        this.serializer = serializer;
    }

    public void writeElement(E element) throws IOException {
        if (!buffer.hasRemaining()) {
            buffer.rewind();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            buffer.rewind();
        }
        Utils.writeElement(serializer, buffer, element);
    }

    @Override
    public void close() throws IOException {
        buffer.limit(buffer.position());
        buffer.rewind();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.close();
    }
}
