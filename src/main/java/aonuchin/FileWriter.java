package aonuchin;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class FileWriter implements Closeable {
    public final Path outputPath;
    public final ByteBuffer buffer;
    public final ByteChannel channel;

    public FileWriter(Path outputPath, ByteBuffer buffer) throws IOException {
        this.outputPath = outputPath;
        this.buffer = buffer;
        this.channel = Files.newByteChannel(outputPath, WRITE, CREATE);
        BufferUtils.resetBuffer(buffer);
    }

    public void writeLong(long number) throws IOException {
        if (!BufferUtils.hasRemaining(buffer)) {
            BufferUtils.resetBuffer(buffer);
            while(channel.write(buffer) > 0);
            BufferUtils.resetBuffer(buffer);
        }
        buffer.putLong(number);
    }

    @Override
    public void close() throws IOException {                               ;
        buffer.limit(buffer.position());
        buffer.rewind();
        while (channel.write(buffer) > 0);
        channel.close();
    }
}
