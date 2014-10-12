package aonuchin;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class ExternalMemorySort {
    private static Random random = new Random();
    private final ByteBuffer readBuffer;
    private final ByteBuffer bigBuffer;
    private final double alfa;
    private final int bufferCount;
    private final Path inputPath;
    private final Path tmpDir;
    private final long pivot;

    public ExternalMemorySort(ByteBuffer readBuffer, ByteBuffer bigBuffer, int bufferCount, Path inputPath, Path tmpDir, long pivot) throws IOException {
        this.readBuffer = readBuffer;
        this.bigBuffer = bigBuffer;
        this.bufferCount = bufferCount;
        this.inputPath = inputPath;
        this.tmpDir = tmpDir;
        this.pivot = pivot;
        alfa = (double) bigBuffer.capacity() / Files.size(inputPath);
    }

    private static Pattern CHUNK_PATTERN = Pattern.compile("chunk-file-(-?\\d+)\\.\\d+");

    public void sort() throws IOException {
        Preconditions.checkArgument(Files.exists(inputPath) &&
                Files.exists(tmpDir) &&
                Files.isDirectory(tmpDir),
                inputPath.toString() + " " + tmpDir.toString());
        if (Files.size(inputPath) < bigBuffer.capacity()) {
            try (ByteChannel channel = Files.newByteChannel(inputPath)) {
                BufferUtils.resetBuffer(bigBuffer);
                while (channel.read(bigBuffer) >= 0) ;
            }
            bigBuffer.limit(bigBuffer.position());
            bigBuffer.rewind();
            ByteBuffer slice = bigBuffer.slice();
            BufferUtils.sort(slice);
            try (ByteChannel channel = Files.newByteChannel(
                    Paths.get(tmpDir.toString(), "sorted-chunk-file-" + pivot + ".0"), WRITE, CREATE)) {
                BufferUtils.resetBuffer(slice);
                while (channel.write(slice) > 0) ;
            }
            return;
        }
        sample();
        bigBuffer.limit(bigBuffer.capacity());
        bigBuffer.rewind();
        BufferUtils.sort(bigBuffer);

        NavigableMap<Long, List<FileWriter>> writersPerPivot = buildPivots();
        try (ByteChannel channel = Files.newByteChannel(inputPath)) {
            int readBytes;
            while ((readBytes = channel.read(readBuffer)) >= 0) {
                readBuffer.limit(readBytes);
                readBuffer.rewind();
                while (BufferUtils.hasRemaining(readBuffer)) {
                    long number = readBuffer.getLong();
                    Entry<Long, List<FileWriter>> entry = writersPerPivot.floorEntry(number);
                    List<FileWriter> writers = entry.getValue();
                    long pivot = entry.getKey();
                    Preconditions.checkArgument(!writers.isEmpty());
                    if (writers.size() == 1) {
                        writers.get(0).writeLong(number);
                    } else if (number > pivot) {
                        writers.get(writers.size() - 1).writeLong(number);
                    } else {
                        writers.get(random.nextInt(writers.size() - 1)).writeLong(number);
                    }
                }
                readBuffer.limit(readBuffer.capacity());
                readBuffer.rewind();
            }
        }
        for (Iterable<FileWriter> writers : writersPerPivot.values()) {
            for (FileWriter writer : writers) {
                writer.close();
            }
        }
        try (DirectoryStream<Path> files = Files.newDirectoryStream(tmpDir, "*chunk-file-*")) {
            for (Path chunkFile : files) {
                Path chunkTmpDir = Paths.get(tmpDir.toString(), "sorted-directory-" + chunkFile.getFileName());
                Files.createDirectories(chunkTmpDir);
                Matcher matcher = CHUNK_PATTERN.matcher(chunkFile.getFileName().toString());
                Preconditions.checkArgument(matcher.find());
                new ExternalMemorySort(readBuffer, bigBuffer,
                        (int) (Files.size(chunkFile) / bigBuffer.capacity()) + 1,
                        chunkFile.toAbsolutePath(),
                        chunkTmpDir.toAbsolutePath(),
                        Long.parseLong(matcher.group(1))).sort();

            }
        }
    }

    private NavigableMap<Long, List<FileWriter>> buildPivots() throws IOException {
        int step = BufferUtils.getLength(bigBuffer) / bufferCount;
        long[] pivots = new long[bufferCount];

        pivots[0] = pivot;
        for (int i = 1; i < bufferCount; i++) {
            pivots[i] = BufferUtils.getValue(bigBuffer, i * step);
        }

        bigBuffer.position(0);
        bigBuffer.limit(readBuffer.capacity());
        NavigableMap<Long, List<FileWriter>> writeBuffers = new TreeMap<>();
        for (long pivot : pivots) {
            if (!writeBuffers.containsKey(pivot)) {
                writeBuffers.put(pivot, new ArrayList<FileWriter>(16));
            }
            List<FileWriter> fileWriters = writeBuffers.get(pivot);
            fileWriters.add(new FileWriter(
                    Paths.get(tmpDir.toString(), "chunk-file-" + pivot + "." + fileWriters.size()),
                    bigBuffer.slice()));
            bigBuffer.position(bigBuffer.limit());
            bigBuffer.limit(bigBuffer.position() + readBuffer.capacity());
        }
        return writeBuffers;
    }

    private void sample() throws IOException {
        while (BufferUtils.hasRemaining(bigBuffer)) {
            try (ByteChannel channel = Files.newByteChannel(inputPath)) {
                int readBytes;
                while ((readBytes = channel.read(readBuffer)) >= 0) {
                    readBuffer.limit(readBytes);
                    readBuffer.rewind();
                    while (BufferUtils.hasRemaining(readBuffer)) {
                        long number = readBuffer.getLong();
                        if (!BufferUtils.hasRemaining(bigBuffer)) {
                            return;
                        }
                        if (random.nextDouble() <= alfa) {
                            bigBuffer.putLong(number);
                        }
                    }
                    BufferUtils.resetBuffer(readBuffer);
                }
            }
        }
    }
}
