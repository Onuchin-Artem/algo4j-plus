package aonuchin.listranking;

import aonuchin.nio.ChannelIterable;
import aonuchin.nio.ChannelIterable.Builder;
import aonuchin.nio.ChannelWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TreeRecursionCalculator<N extends TreeNode, T> {
    private TreeRecursion<N, T> recursion;
    private TreeTraverse<N, T> traverse;

    private Path tmpDir;


    public T calculate(Path inputPath) throws IOException {
        Path traversePath = Paths.get(tmpDir.toString(), "traversedTree.bin");
        traverse.traverseTree(inputPath, traversePath);
        return null;
    }
}
