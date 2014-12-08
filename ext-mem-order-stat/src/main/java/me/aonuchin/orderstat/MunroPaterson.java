package me.aonuchin.orderstat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.util.*;

public class MunroPaterson<T> {
    private final Comparator<T> elementComparator;
    private final OrderedElement<T> infinity = new OrderedElement<T>(true);
    private final Ordering<OrderedElement<T>> comparator = Ordering.from(new Comparator<OrderedElement<T>>() {
        @Override
        public int compare(OrderedElement<T> o1, OrderedElement<T> o2) {
            int isInfinityCompare = Boolean.compare(o1.isInfinity, o2.isInfinity);
            if (isInfinityCompare != 0 || o1.isInfinity) {
                return isInfinityCompare;
            }
            int elementCompare = elementComparator.compare(o1.element, o2.element);
            if (elementCompare != 0) {
                return elementCompare;
            }
            return Long.compare(o1.order, o2.order);
        }
    });

    private int levelSize = 512;

    public static class OrderedElement<T> {
        private final T element;
        private final long order;
        private final boolean isInfinity;

        public OrderedElement(T element, long order) {
            this.element = element;
            this.order = order;
            isInfinity = false;
        }

        public OrderedElement(boolean isInfinity) {
            element = null;
            order = Long.MAX_VALUE;
            this.isInfinity = isInfinity;
        }

        public T getElement() {
            return element;
        }

        public long getOrder() {
            return order;
        }

        public boolean isInfinity() {
            return isInfinity;
        }
    }

    public interface Bounds<T> {
        boolean isInBounds(T element, Comparator<T> comp);

        long updateNumberOfElementsLower(T element, Comparator<T> comp, long oldNumberOfElementsLower);
    }

    public static class InfiniteBounds<T> implements Bounds<T> {
        @Override
        public boolean isInBounds(T element, Comparator<T> comp) {
            return true;
        }

        @Override
        public long updateNumberOfElementsLower(T element, Comparator<T> comp, long oldNumberOfElementsLower) {
            return 0;
        }
    }

    public static class StrictBounds<T> implements Bounds<T> {
        private final T lowerBound;
        private final T upperBound;

        public StrictBounds(T lowerBound, T upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public boolean isInBounds(T element, Comparator<T> comp) {
            return comp.compare(element, lowerBound) >= 0 && comp.compare(element, upperBound) <= 0;
        }

        @Override
        public long updateNumberOfElementsLower(T element, Comparator<T> comp, long oldNumberOfElementsLower) {
            if (comp.compare(element, lowerBound) < 0) {
                return oldNumberOfElementsLower + 1;
            }
            return oldNumberOfElementsLower;
        }
    }

    MunroPaterson(Comparator<T> comparator) {
        this.elementComparator = comparator;
    }

    public static <T> MunroPaterson<T> create(Comparator<T> comparator) {
        return new MunroPaterson<>(comparator);
    }

    public static <T extends Comparable<T>> MunroPaterson<T> create() {
        return create(Ordering.<T>natural());
    }

    public T findKOrderStatistics(ElementsStream.Factory<T> streamFactory, long kOrder) throws IOException {
        StrictBounds<OrderedElement<T>> bounds;
        try (ElementsStream<T> stream = streamFactory.resetAndOpenStream()) {
            bounds = findNewBound(stream, kOrder, new InfiniteBounds<OrderedElement<T>>());
        }
        Preconditions.checkArgument(elementComparator.compare(bounds.lowerBound.element, bounds.upperBound.element) <= 0);
        while (elementComparator.compare(bounds.lowerBound.element, bounds.upperBound.element) < 0) {
            try (ElementsStream<T> stream = streamFactory.resetAndOpenStream()) {
                bounds = findNewBound(stream, kOrder, bounds);
            }
            Preconditions.checkArgument(elementComparator.compare(bounds.lowerBound.element, bounds.upperBound.element) <= 0);
        }
        return bounds.lowerBound.element;
    }

    public StrictBounds<OrderedElement<T>> findNewBound(Iterable<T> stream, long kOrder, Bounds<OrderedElement<T>> oldBounds) {
        ArrayList<OrderedElement<T>> current = new ArrayList<>(levelSize);
        List<ArrayList<OrderedElement<T>>> levels = new ArrayList<>(levelSize);
        long elementOrder = 0;
        long elementSmallerThanBound = 0;
        OrderedElement<T> mostUpperElement = null;
        OrderedElement<T> mostLowerElement = null;
        for (T unorderedElement : stream) {
            OrderedElement<T> element = new OrderedElement<>(unorderedElement, elementOrder++);

            elementSmallerThanBound = oldBounds.updateNumberOfElementsLower(element, comparator, elementSmallerThanBound);
            if (!oldBounds.isInBounds(element, comparator)) {
                continue;
            }
            mostUpperElement = max(mostUpperElement, element);
            mostLowerElement = min(mostLowerElement, element);
            if (isLevelFull(current)) {
                current = mergeUp(current, levels);
                Preconditions.checkArgument(current.isEmpty());
            }
            current.add(element);
        }
        finalizeTree(current, levels);
        Preconditions.checkArgument(kOrder < elementOrder, "K-order is bigger than stream size!");

        Preconditions.checkArgument(mostUpperElement != null && mostLowerElement != null);
        Preconditions.checkArgument(kOrder >= elementSmallerThanBound,
                "DEBUG: element before: " + elementSmallerThanBound + " order: " + kOrder);
        kOrder -= elementSmallerThanBound;
        if (levels.size() == 1) {
            OrderedElement<T> element = levels.get(0).get((int) kOrder);
            return new StrictBounds<>(element, element);
        }

        List<OrderedElement<T>> upperLevel = levels.get(levels.size() - 1);

        assert comparator.isOrdered(upperLevel);
        int lowerIndex = (int) ((kOrder ) / powerOfTwo(levels.size() - 1));
        int upperIndex = (int) ((kOrder ) / powerOfTwo(levels.size() - 1)) + levels.size() - 1;
        OrderedElement<T> lowerElement = lowerIndex >= 0 ?
                upperLevel.get(lowerIndex) : mostLowerElement;
        OrderedElement<T> upperElement = upperIndex < levelSize && !upperLevel.get(upperIndex).isInfinity() ?
                upperLevel.get(upperIndex) : mostUpperElement;
        System.gc();
        return new StrictBounds<>(
                lowerElement,
                upperElement);
    }

    public static long powerOfTwo(int i) {
        return  1L << i;
    }

    private OrderedElement<T> min(OrderedElement<T> mostLowerElement, OrderedElement<T> element) {
        if (mostLowerElement == null) {
            return element;
        }
        return comparator.min(mostLowerElement, element);
    }

    private OrderedElement<T> max(OrderedElement<T> mostUpperElement, OrderedElement<T> element) {
        if (mostUpperElement == null) {
            return element;
        }
        return comparator.max(mostUpperElement, element);
    }

    private boolean isLevelFull(ArrayList<OrderedElement<T>> current) {
        return current.size() == levelSize;
    }

    public void finalizeTree(ArrayList<OrderedElement<T>> current, List<ArrayList<OrderedElement<T>>> levels) {
        for (int i = current.size(); i < levelSize; i++) {
            current.add(infinity);
        }
        current = mergeUp(current, levels);
        int i = 0;
        while (i < levels.size() - 1) {
            ArrayList<OrderedElement<T>> level = levels.get(i);
            if (level.isEmpty()) {
                i++;
                continue;
            }
            for (int j = 0; j < levelSize; j++) {
                current.add(infinity);
            }
            current = mergeUp(current, levels, i);
            i++;
        }
        assert isTreeFull(levels);
    }

    private boolean isTreeFull(List<ArrayList<OrderedElement<T>>> levels) {
        if (!isLevelFull(levels.get(levels.size() - 1))) {
            return false;
        }
        for (int i = 0; i < levels.size() - 1; i++) {
            if (!levels.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private ArrayList<OrderedElement<T>> mergeUp(ArrayList<OrderedElement<T>> current, List<ArrayList<OrderedElement<T>>> levels) {
        return mergeUp(current, levels, 0);
    }

    private ArrayList<OrderedElement<T>> mergeUp(ArrayList<OrderedElement<T>> current, List<ArrayList<OrderedElement<T>>> levels, int levelFrom) {
        ArrayList<OrderedElement<T>> merged = new ArrayList<>(levelSize);

        Preconditions.checkArgument(isLevelFull(current), "DEBUG: " + current.size());
        Preconditions.checkArgument(merged.isEmpty());
        Collections.sort(current, comparator);
        for (int i = levelFrom; i < levels.size(); i++) {
            ArrayList<OrderedElement<T>> level = levels.get(i);
            if (level.isEmpty()) {
                levels.set(i, current);
                return level;
            }
            for (OrderedElement<T> mergeElement : Iterables.mergeSorted(Arrays.asList(
                    Iterables.filter(current, new SkippingFilter()),
                    Iterables.filter(level, new SkippingFilter())), comparator)) {
                merged.add(mergeElement);
            }
            current.clear();
            level.clear();
            ArrayList<OrderedElement<T>> tmp = current;
            current = merged;
            merged = tmp;
            Preconditions.checkArgument(level.isEmpty());
            Preconditions.checkArgument(merged.isEmpty());
            Preconditions.checkArgument(isLevelFull(current));

        }
        levels.add(current);
        Preconditions.checkArgument(merged.isEmpty());
        return merged;
    }

    private class SkippingFilter implements com.google.common.base.Predicate<OrderedElement<T>> {
        int i = 0;
        @Override
        public boolean apply(OrderedElement<T> input) {
            i++;
            return i % 2 == 1;
        }
    }
}
