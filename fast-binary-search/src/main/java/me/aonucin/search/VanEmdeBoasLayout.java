package me.aonucin.search;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.util.*;

/**
* Created by aonuchin on 25.11.14.
*/
public class VanEmdeBoasLayout implements LongStaticSearch {
    private long[] layout;

    public VanEmdeBoasLayout(int size) {
        this.layout = new long[size * 2];
    }

    public VanEmdeBoasLayout(long[] elements) {
        this.layout = elements;
    }

    public void setValue(int pos, long value) {
        layout[2 * pos] = value;
    }

    public void setLeftChildPointer(int pos, int leftChildPtr) {
        long pointers = layout[2 * pos + 1];
        layout[2 * pos + 1] = ((long) leftChildPtr << 32) |
                (pointers & 0x00000000FFFFFFFFL);
    }

    public void setRightChildPointer(int pos, int rightChildPtr) {
        long pointers = layout[2 * pos + 1];
        layout[2 * pos + 1] = ((long) rightChildPtr) |
                (pointers & 0xFFFFFFFF00000000L);
    }

    public long getValue(int pos) {
        return layout[2 * pos];
    }

    public int getLeftChildPointer(int pos) {
        long pointers = layout[2 * pos + 1];
        return (int) ((pointers & 0xFFFFFFFF00000000L) >> 32);
    }

    public int getRightChildPointer(int pos) {
        long pointers = layout[2 * pos + 1];
        return (int) (pointers & 0x00000000FFFFFFFFL);
    }

    @Override
    public boolean contains(long element) {
        if (layout.length == 0) {
            return false;
        }
        int currentPos = 0;
        while (currentPos >= 0) {
            long candidate = getValue(currentPos);
            if (candidate == element) {
                return true;
            }
            if (element < candidate) {
                currentPos = getLeftChildPointer(currentPos);
            } else {
                currentPos = getRightChildPointer(currentPos);
            }
        }
        return false;
    }

    List<Long> subListView(final int from, final int to) {
        return new AbstractList<Long>() {
            @Override
            public Long get(int index) {
                Preconditions.checkArgument(0 <= index && index < size());
                return getValue(from + index);
            }

            @Override
            public int size() {
                return to - from;
            }
        };
    }

    public static VanEmdeBoasLayout build(long[] elements, boolean checkInvariants) {
        VanEmdeBoasLayout layout = new VanEmdeBoasLayout(elements.length);
        Arrays.sort(elements);
        layout.setElements(elements);
        buildLayout(layout, 0, elements.length, layout.getValue(elements.length / 2), new VanEmdeBoasLayout(elements.length), checkInvariants);
        return layout;
    }

    public static VanEmdeBoasLayout wrap(long[] elements) {
        return new VanEmdeBoasLayout(elements);
    }

    private static void buildLayout(VanEmdeBoasLayout layout, int from, int to, long root, VanEmdeBoasLayout buffer, boolean checkInvariants) {
        int length = to - from;
        if (length == 0) {
            return;
        }
        if (checkInvariants) {
            Preconditions.checkArgument(Ordering.natural().isOrdered(layout.subListView(from, to)));
        }
        if (length == 1) {
            if (checkInvariants && from > 0) {
                checkThatElementHasExactlyOneParent(layout, from);
            }
            layout.setLeftChildPointer(from, -1);
            layout.setRightChildPointer(from, -1);
            return;
        }
        layout.copyTo(buffer, from, 0, length);
        int height = LayoutUtil.log2(length);
        int topHalfHeight = height / 2;
    }

    private static int findRootInInOrderLayout(VanEmdeBoasLayout layout, long root, int from, int to) {
        return -1;
    }

    private void copyTo(VanEmdeBoasLayout target, int sourceOffset, int targetOffset, int length) {
        System.arraycopy(layout, sourceOffset * 2, target.layout, targetOffset * 2, length * 2);
    }

    private static void checkThatElementHasExactlyOneParent(VanEmdeBoasLayout layout, int elementPos) {
        long element = layout.getValue(elementPos);
        int parentsNum = 0;
        for (int i = 0; i < elementPos; i++) {
            if (layout.getLeftChildPointer(i) == elementPos) {
                parentsNum++;
                Preconditions.checkArgument(layout.getValue(i) >= element);
            }
            if (layout.getRightChildPointer(i) == elementPos) {
                parentsNum++;
                Preconditions.checkArgument(layout.getValue(i) < element);
            }
        }
        Preconditions.checkArgument(parentsNum == 1);
    }

    public void setElements(long[] elements) {
        for (int i = 0; i < elements.length; i++) {
            setValue(i, elements[i]);
        }
    }
}
