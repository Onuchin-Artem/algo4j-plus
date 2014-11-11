package aonuchin.nio;

import java.nio.ByteBuffer;

public class LongSerializer implements ElementSerializer<Long> {
    @Override
    public int elementSize() {
        return 8;
    }

    @Override
    public void writeElement(ByteBuffer byteBuffer, int offset, Long element) {
        byteBuffer.putLong(offset, element);
    }

    @Override
    public Long readElement(ByteBuffer byteBuffer, int offset) {
        return byteBuffer.getLong(offset);
    }
}
