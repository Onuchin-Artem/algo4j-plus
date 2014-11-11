package aonuchin.nio;

import java.nio.ByteBuffer;

public interface ElementSerializer<E> {
    public int elementSize();

    void writeElement(ByteBuffer byteBuffer, int offset, E element);

    E readElement(ByteBuffer byteBuffer, int offset);
}
