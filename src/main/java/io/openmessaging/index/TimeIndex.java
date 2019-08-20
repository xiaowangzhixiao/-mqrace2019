package io.openmessaging.index;

import static io.openmessaging.utils.BinarySearch.binarySearch;

/**
 * 二级索引
 */
public class TimeIndex {
    private long[] baseTime;
    private int[] offset;
    private int nums = 0;

    public TimeIndex(int capacity) {
        baseTime = new long[capacity];
        offset = new int[capacity];
    }

    public void put(long baseTime, int offset) {
        this.baseTime[nums] = baseTime;
        this.offset[nums] = offset;
        nums++;
    }

    public int search(long t) {
        return binarySearch(t, nums, baseTime);
    }

    public long getBaseTime(int index) {
        if (index >= nums) {
            return Long.MAX_VALUE;
        }
        return baseTime[index];
    }

    public int getOffset(int index) {
        if (index >= nums) {
            return Integer.MAX_VALUE;
        }
        return offset[index];
    }

}
