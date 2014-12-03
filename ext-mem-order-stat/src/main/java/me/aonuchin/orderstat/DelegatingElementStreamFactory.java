package me.aonuchin.orderstat;

import java.io.IOException;
import java.util.Iterator;

public class DelegatingElementStreamFactory<T> implements ElementsStream.Factory<T> {
    private final Iterable<T> iterable;

    public DelegatingElementStreamFactory(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public ElementsStream<T> resetAndOpenStream() throws IOException {
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
}
