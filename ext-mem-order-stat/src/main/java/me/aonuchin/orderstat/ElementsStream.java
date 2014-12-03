package me.aonuchin.orderstat;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by aonuchin on 02.12.14.
 */
public interface ElementsStream<T> extends Iterable<T>, Closeable {
    interface Factory<T> {
        ElementsStream<T> resetAndOpenStream() throws IOException;
    }
}
