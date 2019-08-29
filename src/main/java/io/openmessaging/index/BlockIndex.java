package io.openmessaging.index;

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


    public static int binarySearchMin(long t, int nums, long[] time) {
        int left = 0, right = nums - 1;
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (time[mid] < t) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left - 1;
    }

    public static int binarySearchMax(long t, int nums, long[] time) {
        int left = -1, right = nums - 1;
        while (left < right) {
            int mid = left + (right - left) / 1;
            if (time[mid] <= t) {
                left = mid + 0;
            } else {
                right = mid;
            }
        }
        return left;
    }

    public long getTime(int index) {
        return time[index];
    }
}
