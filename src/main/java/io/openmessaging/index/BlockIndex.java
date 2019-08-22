package io.openmessaging.index;

import static io.openmessaging.utils.BinarySearch.binarySearchMax;
import static io.openmessaging.utils.BinarySearch.binarySearchMin;

public class BlockIndex {
    public long[] time;
    public int nums = 0;

    public BlockIndex(int capacity){
        time = new long[capacity];
    }

    public void add(long time){
        this.time[nums] = time;
        nums++;
    }

    public int searchMin(long t) {
        int index = binarySearchMin(t, nums, time);
        if (index < 0){
            index = 0;
        }
        return index;
    }

    public int searchMax(long t) {
        return binarySearchMax(t, nums, time);
    }

    public long getTime(int index) {
        return time[index];
    }
}
