package aonuchin;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Pipe;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import static java.nio.file.StandardOpenOption.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileTransformer {
    public static void textToBinaryNumbers(Path textFile, Path binaryFile, ByteBuffer bb) throws IOException {
        bb.limit(bb.capacity());
        bb.rewind();
        try (BufferedReader input = Files.newBufferedReader(textFile, Charset.forName("ASCII"))) {
            try (ByteChannel channel = Files.newByteChannel(binaryFile, WRITE, CREATE)) {
                String line;
                while ((line = input.readLine()) != null) {
                    if (bb.position() == bb.limit()) {
                        bb.rewind();
                        while (channel.write(bb) > 0) ;
                        bb.rewind();
                    }
                    bb.putLong(Long.parseLong(line));
                }
                bb.limit(bb.position());
                bb.rewind();
                channel.write(bb);
            }
        }
    }

    public static final Pattern SORTED_CHUNK_PATTERN = Pattern.compile("sorted-chunk-file-(-?\\d+)\\.(\\d+)");
    public static final Comparator<Path> PATH_COMPARATOR = new Comparator<Path>() {
        @Override
        public int compare(Path o1, Path o2) {
            Matcher matcher1 = SORTED_CHUNK_PATTERN.matcher(o1.getFileName().toString());
            Matcher matcher2 = SORTED_CHUNK_PATTERN.matcher(o2.getFileName().toString());
            Preconditions.checkArgument(matcher1.find() && matcher2.find());
            long pivot1 = Long.parseLong(matcher1.group(1));
            long pivot2 = Long.parseLong(matcher2.group(1));
            int pivotCompare = Long.compare(pivot1, pivot2);
            if (pivotCompare != 0) {
                return pivotCompare;
            }
            int order1 = Integer.parseInt(matcher1.group(2));
            int order2 = Integer.parseInt(matcher2.group(2));
            return Integer.compare(order1, order2);
        }
    };

    private static List<Path> findSortedChunks(Path dir) throws IOException {
        final List<Path> sortedChunks = new ArrayList<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (SORTED_CHUNK_PATTERN.matcher(file.getFileName().toString()).matches()) {
                    sortedChunks.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        Collections.sort(sortedChunks, PATH_COMPARATOR);
        return sortedChunks;
    }

    public static void directoryToSortedBinaryFile(Path dir, Path outputFile, ByteBuffer bb) throws IOException {
        final List<Path> sortedChunks = findSortedChunks(dir);
        clueFiles(sortedChunks, outputFile, bb);
    }

    private static void clueFiles(List<Path> sortedChunks, Path outputFile, ByteBuffer bb) throws IOException {
        try (ByteChannel output = Files.newByteChannel(outputFile, WRITE, CREATE)) {
            for (Path path : sortedChunks) {
                try (ByteChannel input = Files.newByteChannel(path)) {
                    int readBytes;
                    while ((readBytes = input.read(bb)) >= 0) {
                        bb.limit(readBytes);
                        bb.rewind();
                        while (BufferUtils.hasRemaining(bb)) {
                            output.write(bb);
                        }
                        BufferUtils.resetBuffer(bb);
                    }
                }
            }
        }
    }


    public static void binaryNumbersToText(Path binaryFile, Path textFile, ByteBuffer bb) throws IOException {
        bb.limit(bb.capacity());
        bb.rewind();
        try (BufferedWriter output = Files.newBufferedWriter(textFile, StandardCharsets.US_ASCII)) {
            try (ByteChannel channel = Files.newByteChannel(binaryFile)) {
                int readBytes;
                while ((readBytes = channel.read(bb)) >= 0) {
                    bb.limit(readBytes);
                    bb.rewind();
                    while (BufferUtils.hasRemaining(bb)) {
                        output.write(bb.getLong() + "\n");
                    }
                    bb.limit(bb.capacity());
                    bb.rewind();

                }
            }
            output.flush();
        }
    }

    public static void generateTestData(Path textFile, long size) throws IOException {
        Random random = new Random();
        try (BufferedWriter output = Files.newBufferedWriter(textFile, StandardCharsets.US_ASCII)) {
            for (long i = 0; i < size; i++) {
                output.write(random.nextLong() + "\n");
            }
        }
    }
}
