package aonuchin;

import java.nio.ByteBuffer;

public class BufferUtils {

   public static void sort(ByteBuffer x) {

       sort1(x, 0, (x.limit() - x.position()) / 8);
   }
  /*
     * The code for each of the seven primitive types is largely identical.
     * C'est la vie.
     */

    /**
     * Sorts the specified sub-array of longs into ascending order.
     */
    private static void sort1(ByteBuffer x, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++)
                for (int j = i; j > off && getValue(x, j - 1) > getValue(x, j); j--)
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
        long v = getValue(x, m);

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && getValue(x, b) <= v) {
                if (getValue(x, b) == v)
                    swap(x, a++, b);
                b++;
            }
            while (c >= b && getValue(x, c) >= v) {
                if (getValue(x, c) == v)
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
        if ((s = b - a) > 1)
            sort1(x, off, s);
        if ((s = d - c) > 1)
            sort1(x, n - s, s);
    }

    /**
     * Swaps getValue(x, a) with getValue(x, b).
     */
    private static void swap(ByteBuffer x, int a, int b) {
        long t = getValue(x, a);
        setValue(x, a, getValue(x, b));
        setValue(x, b, t);
    }

    public static long getValue(ByteBuffer x, int b) {
        return x.getLong(x.position() + b * 8);
    }

    public static void setValue(ByteBuffer x, int b, long t) {
        x.putLong(x.position() + b * 8, t);
    }

    public static int getLength(ByteBuffer x) {
        return x.capacity() / 8;
    }
    /**
     * Swaps getValue(x, a .. (a+n-1)) with getValue(x, b .. (b+n-1)).
     */
    private static void vecswap(ByteBuffer x, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++)
            swap(x, a, b);
    }

    public static void resetBuffer(ByteBuffer x) {
        x.rewind();
        x.limit(x.capacity());
    }
    /**
     * Returns the index of the median of the three indexed longs.
     */
    private static int med3(ByteBuffer x, int a, int b, int c) {
        return (getValue(x, a) < getValue(x, b) ?
                (getValue(x, b) < getValue(x, c) ? b : getValue(x, a) < getValue(x, c) ? c : a) :
                (getValue(x, b) > getValue(x, c) ? b : getValue(x, a) > getValue(x, c) ? c : a));
    }

    public static void merge(ByteBuffer x, int secondBufferStart) {


    }

    public static boolean hasRemaining(ByteBuffer bb) {
        return bb.limit() - bb.position() >= 8;
    }
}
