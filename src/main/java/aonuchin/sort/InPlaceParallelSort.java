package aonuchin.sort;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

// Code was taken from JDK6 Arrays.sort and then modified to be parallel and work with List
public class InPlaceParallelSort {
    private static final int DEFAULT_THRESHOLD = 500000;
    private static ForkJoinPool forkJoin = new ForkJoinPool();

    public static <T> void sort(List<T> list, Comparator<T> comparator) {
        sort(list, comparator, DEFAULT_THRESHOLD);
    }

    public static <T> void sort(List<T> list, Comparator<T> comparator, int threshold) {
        Preconditions.checkArgument(list instanceof RandomAccess);
        try {
            forkJoin.submit(new ParallelSorter<>(list, comparator, 0, list.size(), threshold)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ParallelSorter<T> extends RecursiveAction {
        private final List<T> x;
        private final Comparator<T> comparator;
        private final int off;
        private final int len;
        private final int threshold;

        private ParallelSorter(List<T> x, Comparator<T> comparator, int off, int len, int threshold) {
            this.x = x;
            this.comparator = comparator;
            this.off = off;
            this.len = len;
            this.threshold = threshold;
        }

        protected void compute() {
            // Insertion sort on smallest arrays
            if (len < 7) {
                for (int i = off; i < len + off; i++)
                    for (int j = i; j > off && comparator.compare(x.get(j-1), x.get(j)) > 0; j--)
                        swap(x, j, j - 1);
                return;
            }

            // Choose a partition element, v
            int m = off + (len >> 1);       // Small arrays, middle element
            if (len > 7) {
                int l = off;
                int n = off + len - 1;
                if (len > 40) {        // Big arrays, pseudomedian of 9
                    int s = len / 8;
                    l = med3(x, l, l + s, l + 2 * s);
                    m = med3(x, m - s, m, m + s);
                    n = med3(x, n - 2 * s, n - s, n);
                }
                m = med3(x, l, m, n); // Mid-size, med of 3
            }
            T v = x.get(m);

            // Establish Invariant: v* (<v)* (>v)* v*
            int a = off, b = a, c = off + len - 1, d = c;
            while (true) {
                while (b <= c && comparator.compare(x.get(b), v) <= 0) {
                    if (x.get(b).equals(v))
                        swap(x, a++, b);
                    b++;
                }
                while (c >= b && comparator.compare(x.get(c), v) >= 0) {
                    if (x.get(c).equals(v))
                        swap(x, c, d--);
                    c--;
                }
                if (b > c)
                    break;
                swap(x, b++, c--);
            }

            // Swap partition elements back to middle
            int s, n = off + len;
            s = Math.min(a - off, b - a);
            vecswap(x, off, b - s, s);
            s = Math.min(d - c, n - d - 1);
            vecswap(x, b, n - s, s);

            // Recursively sort non-partition-elements
            if (len < threshold || (b - a) < threshold / 3 || (d - c) < threshold / 3) {
                if ((b - a) > 1) {
                    new ParallelSorter<>(x, comparator, off, b - a, threshold).compute();
                }
                if ((d - c) > 1) {
                    new ParallelSorter<>(x, comparator, n - (d - c), (d - c), threshold).compute();
                }
            } else {
                invokeAll(
                        new ParallelSorter<>(x, comparator, off, b - a, threshold),
                        new ParallelSorter<>(x, comparator, n - (d - c), (d - c), threshold));
            }
        }

        /**
         * Returns the index of the median of the three indexed Ts.
         */
        private int med3(List<T> x, int a, int b, int c) {
            return (comparator.compare(x.get(a), x.get(b)) < 0 ?
                    (comparator.compare(x.get(b), x.get(c)) < 0 ? b : comparator.compare(x.get(a), x.get(c)) < 0 ? c : a) :
                    (comparator.compare(x.get(b), x.get(c)) > 0 ? b : comparator.compare(x.get(a), x.get(c)) > 0 ? c : a));
        }
    }

    /**
     * Swaps x.get(a) with x.get(b).
     */
    private static <T> void swap(List<T> x, int a, int b) {
        T t = x.get(a);
        x.set(a, x.get(b));
        x.set(b, t);
    }

    /**
     * Swaps x.get(a .. (a+n-1)) with x.get(b .. (b+n-1)).
     */
    private static <T> void vecswap(List<T> x, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++)
            swap(x, a, b);
    }
}