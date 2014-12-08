package me.aonucin.search;

import com.google.common.base.Preconditions;

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
                ((int) pointers & 0x00000000FFFFFFFFL);
    }

    public void setRightChildPointer(int pos, int rightChildPtr) {
        long pointers = layout[2 * pos + 1];
        layout[2 * pos + 1] = ((long) (int) (pointers >> 32) << 32) |
                (rightChildPtr & 0x00000000FFFFFFFFL);
    }

    public long getValue(int pos) {
        return layout[2 * pos];
    }

    public int getLeftChildPointer(int pos) {
        long pointers = layout[2 * pos + 1];
        return (int) (pointers >> 32);
    }

    public int getRightChildPointer(int pos) {
        long pointers = layout[2 * pos + 1];
        return (int) pointers;
    }

    @Override
    public boolean contains(long element) {
        if (layout.length == 0) {
            return false;
        }
        int currentPos = 0;
        while (currentPos >= 0) {
            int rightChildPointer = getRightChildPointer(currentPos);
            int leftChildPointer = getLeftChildPointer(currentPos);
            if (rightChildPointer == -2) {
                currentPos = leftChildPointer;
                continue;
            }
            long candidate = getValue(currentPos);
            if (candidate == element) {
                return true;
            }
            if (element < candidate) {
                currentPos = leftChildPointer;
            } else {
                currentPos = rightChildPointer;
            }
        }
        return false;
    }


    private static class OrderedElement implements Comparable<OrderedElement> {
        private long element;
        private int order;
        private boolean isInfinity = false;

        public OrderedElement(long element, int order) {
            this.element = element;
            this.order = order;
        }

        public OrderedElement(int order) {
            this.isInfinity = true;
            this.order = order;
        }

        @Override
        public int compareTo(OrderedElement o) {
            int infinityCompare = Boolean.compare(isInfinity(), o.isInfinity());
            if (infinityCompare != 0 || isInfinity()) {
                return infinityCompare != 0 ? infinityCompare : Integer.compare(order, o.order);
            }
            int elementsCompare = Long.compare(element, o.element);
            return elementsCompare != 0 ? elementsCompare : Integer.compare(order, o.order);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OrderedElement that = (OrderedElement) o;
            return element == that.element && order == that.order && isInfinity == that.isInfinity;
        }

        @Override
        public int hashCode() {
            return 31 * (int) (element ^ (element >>> 32)) + order + Boolean.hashCode(isInfinity);
        }

        public boolean isInfinity() {
            return isInfinity;
        }
    }

    public static class Builder {
        List<Long> elements = new ArrayList<>();

        Builder addElement(long element) {
            elements.add(element);
            return this;
        }

        VanEmdeBoasLayout build() {
            long[] elementsArr = new long[elements.size()];
            for (int i = 0; i < elementsArr.length; i++) {
                elementsArr[i] = elements.get(i);
            }
            return VanEmdeBoasLayout.build(elementsArr);
        }
    }
    private static class VEBNode {
        private OrderedElement element;
        private int leftChildPos;
        private int rightChildPos;

        public VEBNode(OrderedElement element, int leftChildPos, int rightChildPos) {
            this.element = element;
            this.leftChildPos = leftChildPos;
            this.rightChildPos = rightChildPos;
        }

        public VEBNode(OrderedElement orderedElement) {
            this.element = orderedElement;
        }
    }

    public static VanEmdeBoasLayout build(long[] elements) {
        OrderedElement[] bfsLayout = buildBFSLayout(elements);
        VEBNode[] vebLayout = buildVEBLayout(bfsLayout);
        VanEmdeBoasLayout vebLayoutLocal = new VanEmdeBoasLayout(vebLayout.length);
        for (int i = 0; i < vebLayout.length; i++) {
            VEBNode node = vebLayout[i];
            vebLayoutLocal.setValue(i, node.element.element);
            if (node.leftChildPos != 0) {
                vebLayoutLocal.setLeftChildPointer(i, node.leftChildPos);
            } else {
                vebLayoutLocal.setLeftChildPointer(i, -1);
            }
            if (node.rightChildPos != 0) {
                vebLayoutLocal.setRightChildPointer(i, node.rightChildPos);
            } else {
                vebLayoutLocal.setRightChildPointer(i, -1);
            }
        }
        return vebLayoutLocal;
    }

    private static OrderedElement[] buildBFSLayout(long[] elements) {
        OrderedElement[] sorted = new OrderedElement[roundUpToNextPowerOfTwo(elements.length + 1) - 1];
        for (int i = 0; i < elements.length; i++) {
            sorted[i] = new OrderedElement(elements[i], i);
        }
        for (int i = elements.length; i < sorted.length; i++) {
            sorted[i] = new OrderedElement(i);
        }
        Arrays.sort(sorted);
        OrderedElement[] layout = Arrays.copyOf(sorted, sorted.length);
        if (layout.length == 0) {
            return layout;
        }
        buildBFSLayout(sorted, layout, 0, layout.length, 0);
        return layout;
    }

    private static void buildBFSLayout(OrderedElement[] sorted, OrderedElement[] layout,
                                       int from, int to, int layoutPos) {
        int medianPos = from + (to - from) / 2;
        layout[layoutPos] = sorted[medianPos];
        if (from < medianPos) {
            buildBFSLayout(sorted, layout, from, medianPos, bfsLeftPos(layoutPos));
        }
        if (medianPos + 1 < to) {
            buildBFSLayout(sorted, layout, medianPos + 1, to, bfsRightPos(layoutPos));
        }
    }

    private static int bfsRightPos(int layoutPos) {
        return 2 * (layoutPos + 1);
    }

    private static int bfsLeftPos(int layoutPos) {
        return 2 * layoutPos + 1;
    }

    private static int findElementInBFSLayout(OrderedElement[] layout, OrderedElement element) {
        int i = 0;
        do {
            if (layout[i].equals(element)) {
                return i;
            }
            if (element.compareTo(layout[i]) < 0) {
                i = bfsLeftPos(i);
            } else {
                i = bfsRightPos(i);
            }
        } while (i < layout.length);
        throw new IllegalStateException();
    }

    private static VEBNode[] buildVEBLayout(OrderedElement[] bfsLayout) {
        VEBNode[] vebLayout = new VEBNode[bfsLayout.length];
        buildVEBLayout(bfsLayout, vebLayout, 0, LayoutUtil.log2(bfsLayout.length + 1), 0, vebLayout.length);
        for (int i = 0; i < vebLayout.length; i++) {
            VEBNode node = vebLayout[i];
            if (node.element.isInfinity()) {
                node.rightChildPos = -2;
            }
            int bfsPos = findElementInBFSLayout(bfsLayout, node.element);
            if (bfsLeftPos(bfsPos) < bfsLayout.length) {
                OrderedElement left = bfsLayout[bfsLeftPos(bfsPos)];
                findChild(vebLayout, i, left, true);
            }
            if (!node.element.isInfinity() && bfsRightPos(bfsPos) < bfsLayout.length) {
                OrderedElement right = bfsLayout[bfsRightPos(bfsPos)];
                findChild(vebLayout, i, right, false);
            }

        }
        return vebLayout;
    }

    private static void findChild(VEBNode[] vebLayout, int pos, OrderedElement child, boolean isLeft) {
        for (int j = pos + 1; j < vebLayout.length; j++) {
            if (vebLayout[j].element.equals(child)) {
                if (isLeft) {
                    vebLayout[pos].leftChildPos = j;
                } else {
                    vebLayout[pos].rightChildPos = j;
                }
                return;
            }
        }
        throw new IllegalStateException();
    }

    // 0 1 2 3 4 5 6
    private static void buildVEBLayout(OrderedElement[] bfsLayout, VEBNode[] vebLayout,
                                       final int bfsRootPosition, final int treeHeight,
                                       final int vebFromPos, final int vebToPos) {
        if (treeHeight == 0) {
            return;
        }
        if (treeHeight == 1) {
            Preconditions.checkArgument(vebToPos - vebFromPos == 1, vebToPos + " " + vebFromPos);
            vebLayout[vebFromPos] = new VEBNode(bfsLayout[bfsRootPosition]);
            return;
        }
        int topHeight = treeHeight / 2;
        int bottomHeight = treeHeight - topHeight;
        int vebTopTreeEnd = vebFromPos + treeSize(topHeight);
        buildVEBLayout(bfsLayout, vebLayout, bfsRootPosition, topHeight, vebFromPos, vebTopTreeEnd);
        int bottomLevelStart = bfsRootPosition;
        for (int i = 0; i < topHeight; i++) {
            bottomLevelStart = bfsLeftPos(bottomLevelStart);
        }
        int bottomLevelEnd = bottomLevelStart + LayoutUtil.powerOfTwo(topHeight);
        int i = 0;
        Preconditions.checkArgument(vebToPos ==  vebTopTreeEnd + LayoutUtil.powerOfTwo(topHeight) * treeSize(bottomHeight));
        for (int bottomRootPos = bottomLevelStart; bottomRootPos < bottomLevelEnd; bottomRootPos++) {
            buildVEBLayout(bfsLayout, vebLayout, bottomRootPos, bottomHeight, vebTopTreeEnd + i * treeSize(bottomHeight), vebTopTreeEnd + (i + 1) * treeSize(bottomHeight));
            i++;
        }
    }

    private static int treeSize(int treeHeight) {
        return LayoutUtil.powerOfTwo(treeHeight) - 1;
    }


    private static int roundUpToNextPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }


}
