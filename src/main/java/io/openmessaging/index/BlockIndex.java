package io.openmessaging.index;

import static io.openmessaging.utils.BinarySearch.binarySearch;

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

    public int search(long t) {
        return binarySearch(t, nums, time);
    }


    public long getTime(int index) {
        return time[index];
    }
}
