package me.aonuchin.orderstat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.util.*;

public class MunroPaterson<T> {
    private final Comparator<T> elementComparator;
    private final Ordering<OrderedElement<T>> comparator = Ordering.from(new Comparator<OrderedElement<T>>() {
        @Override
        public int compare(OrderedElement<T> o1, OrderedElement<T> o2) {
            int compare = elementComparator.compare(o1.element, o2.element);
            if (compare != 0) {
                return compare;
            }
            return Long.compare(o1.order, o2.order);
        }
    });

    private int levelSize = 512;

    public static class OrderedElement<T> {
        private final T element;
        private final long order;

        public OrderedElement(T element, long order) {
            this.element = element;
            this.order = order;
        }

        public T getElement() {
            return element;
        }

        public long getOrder() {
            return order;
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
        ArrayList<OrderedElement<T>> current = new ArrayList<>();
        List<ArrayList<OrderedElement<T>>> levels = new ArrayList<>();
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
        if (isLevelFull(current)) {
            mergeUp(current, levels);
        }
        Preconditions.checkArgument(kOrder < elementOrder, "K-order is bigger than stream size!");

        Preconditions.checkArgument(mostUpperElement != null && mostLowerElement != null);
        Preconditions.checkArgument(kOrder >= elementSmallerThanBound,
                "DEBUG: element before: " + elementSmallerThanBound + " order: " + kOrder);
        kOrder -= elementSmallerThanBound;
        if (levels.isEmpty()) {
            Preconditions.checkArgument(kOrder < current.size(), "DEBUG " + kOrder + " " + current.size());
            Collections.sort(current, comparator);
            return new StrictBounds<>(current.get((int) kOrder), current.get((int) kOrder));
        }

        List<OrderedElement<T>> upperLevel = levels.get(levels.size() - 1);

        assert comparator.isOrdered(upperLevel);
        int lowerIndex = (int) (kOrder / (2 ^ levels.size()));
        int upperIndex = (int) (kOrder / (2 ^ levels.size())) + levels.size();
        return new StrictBounds<>(
                kOrder >= (2 ^ levels.size()) ?  upperLevel.get(lowerIndex) : mostLowerElement,
                upperIndex < levelSize ? upperLevel.get(upperIndex) : mostUpperElement);
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

    private ArrayList<OrderedElement<T>> mergeUp(ArrayList<OrderedElement<T>> current, List<ArrayList<OrderedElement<T>>> levels) {
        ArrayList<OrderedElement<T>> merged = new ArrayList<>(levelSize);

        Preconditions.checkArgument(isLevelFull(current));
        Preconditions.checkArgument(merged.isEmpty());
        Collections.sort(current, comparator);
        for (int i = 0; i < levels.size(); i++) {
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
            ArrayList<OrderedElement<T>> tmp = current;
            current = merged;
            merged = tmp;
            Preconditions.checkArgument(level.isEmpty());
            Preconditions.checkArgument(merged.isEmpty());
            Preconditions.checkArgument(isLevelFull(current));

        }
        levels.add(current);
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
