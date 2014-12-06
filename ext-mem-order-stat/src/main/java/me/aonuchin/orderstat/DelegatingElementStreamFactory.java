package me.aonuchin.orderstat;

import java.io.IOException;
import java.util.Iterator;

public class DelegatingElementStreamFactory<T> implements ElementsStream.Factory<T> {
    private final Iterable<T> iterable;
    private int passesNumber = 0;

    public DelegatingElementStreamFactory(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public ElementsStream<T> resetAndOpenStream() throws IOException {
        passesNumber++;
        return new ElementsStream<T>() {
            @Override
            public void close() throws IOException {
            }

            @Override
            public Iterator<T> iterator() {
                return iterable.iterator();
            }
        };
    }

    public int getPassesNumber() {
        return passesNumber;
    }

    public void resetPasses() {
        passesNumber = 0;
    }
}
