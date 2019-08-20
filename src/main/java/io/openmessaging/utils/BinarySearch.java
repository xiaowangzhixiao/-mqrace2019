package io.openmessaging.utils;

public class BinarySearch {
    public static int binarySearch(long t, int nums, long[] time) {
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

    public static void main(String[] args) {
        long t = 1;
        long[] ts = {2,3,5,5,7,8};
        System.out.println(binarySearch(t, 6 , ts));
    }
}
