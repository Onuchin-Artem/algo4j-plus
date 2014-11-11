package aonuchin.listranking;

import aonuchin.join.ExternalMemoryJoiner;
import aonuchin.nio.ChannelIterable.Builder;
import aonuchin.sort.ExternalMemorySort;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TreeTraverse<N extends TreeNode, T> {
    public static class TraverseEdge<N> {

    }
    private ListRanking ranking;
    private Builder<N> channelIterator;
    private ExternalMemoryJoiner<N, N, Long, TraverseEdge<N>> childJoiner;
    private ExternalMemorySort<N> sorter;
    private Path tmpDir;

    public void traverseTree(Path inputPath, Path outputPath) throws IOException {
        Path sortedByNodeInput = Paths.get(tmpDir.toString(), "sorted-by-node");
        sorter.sort(inputPath, sortedByNodeInput, SORT_BY_);
    }
}
