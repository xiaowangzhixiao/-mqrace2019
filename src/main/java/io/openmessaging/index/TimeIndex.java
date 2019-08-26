package io.openmessaging.index;

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

    public void update(int offset) {
        this.offset[nums-1] = offset;
    }

    public int search(long time) {
        int index = (int)((time - baseTime) >>> 8);
        if (index >= nums){
            return nums-1;
        }
        while (offset[index] == -1){
            index++;
        }

        return index;
    }

    public long getBaseTime(int index) {
        return (long)index << 8 + baseTime;
    }

    public int getOffset(int index) {
        return offset[index];
    }

    public int getNums() {
        return nums;
    }
}
