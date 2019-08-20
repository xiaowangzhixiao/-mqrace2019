package io.openmessaging.index;

/**
 * 索引常驻内存
 */
public class Index {
    private long[] baseTime;
    private int[] offset;
    private int nums = 0;

    public Index(int capacity) {
        baseTime = new long[capacity];
        offset = new int[capacity];
    }

    public int search(long t) {
        int left = 0, right = nums - 1;
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (baseTime[mid] > t) {
                right = mid;
            } else {
                left = mid+1;
            }
        }
        return left - 1;
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
