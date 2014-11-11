package aonuchin.example;

import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
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
                        while (bb.hasRemaining()) {
                            channel.write(bb);
                        }
                        bb.rewind();
                    }
                    bb.putLong(Long.parseLong(line));
                }
                bb.limit(bb.position());
                bb.rewind();
                while (bb.hasRemaining()) {
                    channel.write(bb);
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
                    while (bb.hasRemaining()) {
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
