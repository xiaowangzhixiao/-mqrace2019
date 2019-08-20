package io.openmessaging.index;

import static io.openmessaging.utils.BinarySearch.binarySearch;

public class BlockIndex {
    public long[] time;
    public int[] offset;
    public int[] size;
    public int nums = 0;

    public BlockIndex(int capacity){
        time = new long[capacity];
        offset = new int[capacity];
        size = new int[capacity];
    }

    public void add(long time, int offset, int size){
        this.time[nums] = time;
        this.offset[nums] = offset;
        this.size[nums] = size;
        nums++;
    }

    public int search(long t) {
        return binarySearch(t, nums, time);
    }

    public int getOffset(int index){
        return offset[index];
    }

    public long getTime(int index) {
        return time[index];
    }

    public int getSize(int index) {
        return size[index];
    }
}
