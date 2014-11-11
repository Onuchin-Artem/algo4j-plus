package aonuchin.sort;

import aonuchin.example.ExternalMemorySortLauncher;
import com.google.common.base.Charsets;
import org.junit.Test;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import static org.junit.Assert.*;

public class ExternalMemorySortIntegrationTest {

    @Test
    public void testSort() throws Exception {
        ExternalMemorySortLauncher.main("-dt", "5000000", "-o", "input.txt");
        ExternalMemorySortLauncher.main("-ts", "-i", "input.txt", "-o", "output.txt", "-m", "2500000", "-b", "25000");

        long linesNum = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("output.txt"), Charsets.US_ASCII)) {
            String line;
            long prev = Long.MIN_VALUE;
            while ((line = reader.readLine()) != null) {
                long curr = Long.parseLong(line);
                assertTrue(curr >= prev);
                linesNum++;
            }
        }
        assertEquals(5000000L, linesNum);
    }
}
