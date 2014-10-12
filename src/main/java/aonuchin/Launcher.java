package aonuchin;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class Launcher {
    public static void main(String ... a) throws Exception {
        Options options = new Options();
        options.addOption("i", "input", true, "input path. Required");
        options.addOption("o", "output", true, "output path. Required");
        options.addOption("b", "block-size", true, "size in bytes of i/o block");
        options.addOption("m", "memory-size", true, "size in bytes of memory buffer");
        options.addOption("c", "split-count", true, "number of parts file should be splitted");
        options.addOption("t", "tmp", true, "directory for temporary files");
        options.addOption("dt", "generate-test-data", true, "Generates test data");
        options.addOption("ts", "text-sort", false, "Sorts text file with a number per line");
        options.addOption("t2b", "text-to-binary", false, "converts text to binary file");
        options.addOption("b2t", "binary-to-text", false, "converts binary to text file");
        options.addOption("h", "help", false, "shows this table");
        CommandLineParser parser = new PosixParser();
        CommandLine arguments = parser.parse(options, a);
        if ((!arguments.hasOption("o") &&
             !(arguments.hasOption("dt") || arguments.hasOption("i"))) || arguments.hasOption("h")) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("java -jar sort-jar-with-dependencies.jar", options);
            return;
        }
        Path outputPath = Paths.get(arguments.getOptionValue("o"));
        if (arguments.hasOption("dt")) {
            long size = Long.parseLong(arguments.getOptionValue("dt"));
            FileTransformer.generateTestData(outputPath, size);
            return;
        }
        int bufferSize = Integer.parseInt(arguments.getOptionValue("b", "1048576"));
        int bigBufferSize = Integer.parseInt(arguments.getOptionValue("m", "536870912"));
        Preconditions.checkArgument(bufferSize % 8 == 0);
        Preconditions.checkArgument(bigBufferSize % 16 == 0);
        Preconditions.checkArgument(bigBufferSize > 2 * bufferSize);
        int maxBufferCount = bigBufferSize / bufferSize - 1;
        int bufferCount = Integer.parseInt(arguments.getOptionValue("c", maxBufferCount + ""));
        bufferCount = Math.max(2, bufferCount);
        bufferCount = Math.min(maxBufferCount, bufferCount);
        Path inputPath = Paths.get(arguments.getOptionValue("i"));
        Path tmpDir = Paths.get(arguments.getOptionValue("t", "./sort-tmp"));
        Preconditions.checkArgument(Files.exists(inputPath));
        FileUtils.deleteDirectory(tmpDir.toFile());
        Files.createDirectories(tmpDir);
        ByteBuffer bigBuffer = ByteBuffer.allocateDirect(bigBufferSize);
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(bufferSize);
        Files.deleteIfExists(outputPath);

        if (arguments.hasOption("t2b")) {
            FileTransformer.textToBinaryNumbers(inputPath, outputPath, bigBuffer);
            return;
        }
        if (arguments.hasOption("b2t")) {
            FileTransformer.binaryNumbersToText(inputPath, outputPath, bigBuffer);
            return;
        }
        System.out.println("Parameters: " + Files.size(inputPath) + " " + bigBufferSize + " " + bufferSize + " " + bufferCount);

        if (arguments.hasOption("ts")) {
            Path binInputPath = Paths.get(inputPath.toString() + ".bin");
            Files.deleteIfExists(binInputPath);
            Path binOutputPath = Paths.get(outputPath.toString() + ".bin");
            Files.deleteIfExists(binOutputPath);

            FileTransformer.textToBinaryNumbers(inputPath, binInputPath, bigBuffer);
            new ExternalMemorySort(readBuffer, bigBuffer, bufferCount, binInputPath, tmpDir, Long.MIN_VALUE).sort();
            FileTransformer.directoryToSortedBinaryFile(tmpDir, binOutputPath, bigBuffer);
            FileTransformer.binaryNumbersToText(binOutputPath, outputPath, bigBuffer);
            return;
        }
        new ExternalMemorySort(readBuffer, bigBuffer, bufferCount, inputPath, tmpDir, Long.MIN_VALUE).sort();
        FileTransformer.directoryToSortedBinaryFile(tmpDir, outputPath, readBuffer);
    }
}
