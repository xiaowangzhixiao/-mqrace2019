package io.openmessaging.file;

import io.openmessaging.index.Index;

/**
 * TimeIO
 */
public class TimeIO {
    private Index index = new Index(1000);
    private byte[] time = new byte[Integer.MAX_VALUE / 2];
    private int nums = 0;

    public void put() {
        
    }
}