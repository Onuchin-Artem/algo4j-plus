package aonuchin.sort;

import aonuchin.Utils;
import aonuchin.nio.ByteBuffersList;
import aonuchin.nio.ChannelIterable;
import aonuchin.nio.ChannelIterable.Builder;
import aonuchin.nio.ChannelWriter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class ExternalMemorySort<E> {
    private static class DistributionSort<E> {
        public static final Pattern SORTED_CHUNK_PATTERN = Pattern.compile("sorted-chunk-file-\\d+");
        final Comparator<Path> pathComparator = new Comparator<Path>() {
            @Override
            public int compare(Path o1, Path o2) {
                Matcher matcher1 = SORTED_CHUNK_PATTERN.matcher(o1.getFileName().toString());
                Matcher matcher2 = SORTED_CHUNK_PATTERN.matcher(o2.getFileName().toString());
                Preconditions.checkArgument(matcher1.find() && matcher2.find());
                E pivot1 = pivots.get(o1.getFileName());
                E pivot2 = pivots.get(o2.getFileName());
                return comparator.compare(pivot1, pivot2);
            }
        };
        private final ChannelIterable.Builder<E> channelIterator;
        private final ByteBuffersList<E> list;
        private final int bufferCount;
        private final Path inputPath;
        private final Comparator<E> comparator;
        private final Path tmpDir;
        private final Map<Path, E> pivots;

        public DistributionSort(Comparator<E> comparator, Builder<E> channelIterator, ByteBuffersList<E> bigBuffer, int bufferCount, Path inputPath, Path tmpDir, Map<Path, E> pivots) throws IOException {
            this.channelIterator = channelIterator;
            this.comparator = comparator;
            this.list = bigBuffer;
            this.bufferCount = bufferCount;
            this.inputPath = inputPath;
            this.tmpDir = tmpDir;
            this.pivots = pivots;
        }

        public DistributionSort(Comparator<E> comparator, Builder<E> channelIterator, ByteBuffersList<E> bigBuffer, int bufferCount, Path inputPath, Path tmpDir) throws IOException {
            this(comparator, channelIterator, bigBuffer, bufferCount, inputPath, tmpDir, new HashMap<Path, E>());
        }


        private List<Path> findSortedChunks(Path dir) throws IOException {
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
            Collections.sort(sortedChunks, pathComparator);
            return sortedChunks;
        }

        public void directoryToSortedBinaryFile(Path dir, Path outputFile, ByteBuffer bb) throws IOException {
            final List<Path> sortedChunks = findSortedChunks(dir);
            clueFiles(sortedChunks, outputFile, bb);
        }

        private void clueFiles(List<Path> sortedChunks, Path outputFile, ByteBuffer bb) throws IOException {
            try (ByteChannel output = Files.newByteChannel(outputFile, WRITE, CREATE)) {
                for (Path path : sortedChunks) {
                    try (ByteChannel input = Files.newByteChannel(path)) {
                        bb.limit(bb.capacity());
                        bb.rewind();
                        int totalReadBytes = 0;
                        int readBytes;
                        while ((readBytes = input.read(bb)) >= 0) {
                            totalReadBytes += readBytes;
                            if (readBytes == 0) {
                                bb.rewind();
                                bb.limit(totalReadBytes);
                                while (bb.hasRemaining()) {
                                    output.write(bb);
                                }
                                bb.limit(bb.capacity());
                                bb.rewind();
                                totalReadBytes = 0;
                            }
                        }
                        bb.rewind();
                        bb.limit(totalReadBytes);
                        while (bb.hasRemaining()) {
                            output.write(bb);
                        }
                    }
                }
            }
        }

        public void splitChunksAndSort() throws IOException {
            Preconditions.checkArgument(Files.exists(inputPath) &&
                    Files.exists(tmpDir) &&
                    Files.isDirectory(tmpDir),
                    inputPath.toString() + " " + tmpDir.toString());
            if (Files.size(inputPath) < list.capacityInBytes()) {
                try (ByteChannel channel = Files.newByteChannel(inputPath)) {
                    list.readFromChannel(channel);
                }
                InPlaceParallelSort.sort(list, comparator);
                if (list.isEmpty()) {
                    return;
                }
                Path outputPath = Paths.get(tmpDir.toString(), "sorted-chunk-file-" + pivots.size());
                pivots.put(outputPath.getFileName(), list.get(0));
                try (ByteChannel channel = Files.newByteChannel(
                        outputPath, WRITE, CREATE)) {
                    list.writeToChannel(channel);
                }
                return;
            }
            E min = sampleAndFindMin();
            InPlaceParallelSort.sort(list, comparator);

            NavigableMap<E, List<ChannelWriter<E>>> writersPerPivot = buildPivots(min);
            try (ChannelIterable<E> elementsInFile = channelIterator.iterateOverChannel(Files.newByteChannel(inputPath))) {
                for (E element : elementsInFile) {
                    Entry<E, List<ChannelWriter<E>>> entry = writersPerPivot.floorEntry(element);
                    List<ChannelWriter<E>> writers = entry.getValue();
                    E pivot = entry.getKey();
                    Preconditions.checkArgument(!writers.isEmpty());
                    if (writers.size() == 1) {
                        writers.get(0).writeElement(element);
                    } else if (comparator.compare(element, pivot) > 0) {
                        writers.get(writers.size() - 1).writeElement(element);
                    } else {
                        writers.get(ThreadLocalRandom.current().nextInt(writers.size() - 1)).writeElement(element);
                    }
                }
            }
            for (Iterable<ChannelWriter<E>> writers : writersPerPivot.values()) {
                for (ChannelWriter writer : writers) {
                    writer.close();
                }
            }
            try (DirectoryStream<Path> files = Files.newDirectoryStream(tmpDir, new Filter<Path>() {
                @Override
                public boolean accept(Path entry) throws IOException {
                    return entry.getFileName().toString().startsWith("chunk-file-") && Files.isReadable(entry);
                }
            })) {
                int filesNum = 0;
                for (Path chunkFile : files) {
                    filesNum++;
                    Path chunkTmpDir = Paths.get(tmpDir.toString(), "sorted-directory-" + chunkFile.getFileName());
                    Files.createDirectories(chunkTmpDir);
                    new DistributionSort<E>(comparator, channelIterator, list,
                            (int) (Files.size(chunkFile) / list.capacityInBytes()) + 1,
                            chunkFile.toAbsolutePath(),
                            chunkTmpDir.toAbsolutePath(),
                            pivots).splitChunksAndSort();

                }
                Preconditions.checkArgument(bufferCount == filesNum);
            }
        }

        private NavigableMap<E, List<ChannelWriter<E>>> buildPivots(E minPivot) throws IOException {
            int step = list.size() / bufferCount;
            List<E> pivots = new ArrayList<>(bufferCount);

            pivots.add(minPivot);
            for (int i = 1; i < bufferCount; i++) {
                pivots.add(list.get(i * step));
            }
            Preconditions.checkArgument(pivots.size() == bufferCount);

            NavigableMap<E, List<ChannelWriter<E>>> writeBuffers = new TreeMap<>();
            List<ByteBuffer> buffers = Utils.sliceListBuffersPool(list.getBuffersPool(), channelIterator.bufferSize(), bufferCount);
            int i = 0;
            for (E pivot : pivots) {
                if (!writeBuffers.containsKey(pivot)) {
                    writeBuffers.put(pivot, new ArrayList<ChannelWriter<E>>(16));
                }
                List<ChannelWriter<E>> fileWriters = writeBuffers.get(pivot);
                Path unsortedOutputPath = Paths.get(tmpDir.toString(), "chunk-file-" + i + "." + fileWriters.size());
                fileWriters.add(new ChannelWriter<>(
                        list.getSerializer(), buffers.get(i), Files.newByteChannel(unsortedOutputPath, WRITE, CREATE)));

                i++;
            }
            return writeBuffers;
        }

        E sampleAndFindMin() throws IOException {
            try (ChannelIterable<E> elementsInFile = channelIterator.iterateOverChannel(Files.newByteChannel(inputPath))) {
               return Utils.sampleAndFindMin(elementsInFile, list, list.capacity(), comparator);
            }
        }
    }

    private ChannelIterable.Builder<E> channelIterator;
    private ByteBuffersList<E> list;
    private int bufferCount;
    private Path tmpDir;

    public ExternalMemorySort(Builder<E> channelIterator, ByteBuffersList<E> list, int bufferCount, Path tmpDir) {
        this.channelIterator = channelIterator;
        this.list = list;
        this.bufferCount = bufferCount;
        this.tmpDir = tmpDir;
    }

    public void sort(Path inputPath, Path outputPath, Comparator<E> comparator) throws IOException {
        DistributionSort<E> sorter = new DistributionSort<E>(comparator, channelIterator, list, bufferCount, inputPath, tmpDir);
        sorter.splitChunksAndSort();
        sorter.directoryToSortedBinaryFile(tmpDir, outputPath, channelIterator.getBuffer());
    }
}
