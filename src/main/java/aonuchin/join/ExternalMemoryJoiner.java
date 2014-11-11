package aonuchin.join;

import aonuchin.nio.ChannelWriter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;

public class ExternalMemoryJoiner<L, R, K, O> {

    public interface KeyExtractor<T, K> {
        K getKey(T obj);
    }

    public interface Joiner<L, R, O> {
        O joinObject(L left, R  right);
    }
    private KeyExtractor<L, K> leftKeyExtractor;
    private KeyExtractor<R, K> rightKeyExtractor;
    private Joiner<L, R, O> joiner;
    private Comparator<K> keyComparator;


    public void join(Iterable<L> leftSortedStream, Iterable<R> rightSortedStream, ChannelWriter<O> output) throws IOException {
        Iterator<L> leftIterator = leftSortedStream.iterator();
        Iterator<R> rightIterator = rightSortedStream.iterator();
        L left = null;
        R right = null;
        while (leftIterator.hasNext() && rightIterator.hasNext()) {
            if (left == null) {
                Preconditions.checkArgument(right == null);
                left = leftIterator.next();
                right = rightIterator.next();
                continue;
            }
            int compare = keyComparator.compare(leftKeyExtractor.getKey(left), rightKeyExtractor.getKey(right));
            if (compare == 0) {
                output.writeElement(joiner.joinObject(left, right));
                left = readNext(leftIterator, leftKeyExtractor, left);
                right = readNext(rightIterator, rightKeyExtractor, right);
            } else if (compare < 0) {
                left = readNext(leftIterator, leftKeyExtractor, left);
            } else {
                right = readNext(rightIterator, rightKeyExtractor, right);
            }
        }
        if (left == null) {
            return;
        }
        int compare = keyComparator.compare(leftKeyExtractor.getKey(left), rightKeyExtractor.getKey(right));
        if (compare == 0) {
            output.writeElement(joiner.joinObject(left, right));
        }
    }

    private <T> T readNext(Iterator<T> iterator, KeyExtractor<T, K> keyExtractor, T prev) {
        T newLeft = iterator.next();
        Preconditions.checkArgument(
                keyComparator.compare(keyExtractor.getKey(prev), keyExtractor.getKey(newLeft)) <= 0);
        return newLeft;
    }


}
