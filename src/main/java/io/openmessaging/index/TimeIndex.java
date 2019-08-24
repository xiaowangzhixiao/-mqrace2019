package io.openmessaging.index;

import static io.openmessaging.utils.BinarySearch.binarySearchMin;

/**
 * 二级索引
 */
public class TimeIndex {
    private long baseTime;
    private int[] offset;
    private int nums = 0;

    public TimeIndex(int capacity) {
        offset = new int[capacity];
    }

    public long getBaseTime() {
        return baseTime;
    }

    public void setBaseTime(long baseTime) {
        this.baseTime = baseTime;
    }

    public void put(int offset) {
        this.offset[nums] = offset;
        nums++;
    }

    public int search(long time) {
        return ((int)(time - baseTime)) >>> 8;
    }

    public long getBaseTime(int index) {
        return index << 8 + baseTime;
    }

    public int getOffset(int index) {
        if (index >= nums) {
            return Integer.MAX_VALUE;
        }
        return offset[index];
    }

}
