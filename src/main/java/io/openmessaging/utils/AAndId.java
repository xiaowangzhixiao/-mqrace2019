package io.openmessaging.utils;

import java.nio.ByteBuffer;

public class AAndId {

    public static void writeToByteBuffer(long a, int id, int offset, ByteBuffer byteBuffer){
        byteBuffer.putInt(id);
        byteBuffer.putInt(offset);
        byteBuffer.putLong(a);
    }
}
