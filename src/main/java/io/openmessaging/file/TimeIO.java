package io.openmessaging.file;

import io.openmessaging.index.TimeIndex;
import io.openmessaging.utils.TimeInfo;

import static io.openmessaging.utils.BinarySearch.binarySearchMin;

/**
 * TimeIO
 */
public class TimeIO {
    private TimeIndex index = new TimeIndex(100000000);
    private byte[] times = new byte[Integer.MAX_VALUE / 12];
    private int nums = 0;
    private long baseTime = 0;

    public void put(long time) {
        if (nums == 0){
            index.setBaseTime(time);
            baseTime = time;
        }else if (time >= baseTime + 255){
            baseTime = baseTime + 255;
            index.put(nums);
        }
        this.times[nums] = (byte) (time & 0xff);
        nums++;
    }

    public int getMinIndex(long time) {
        int indexIndex = index.search(time);
        return binarySearchMin(time & 0xff, index.getOffset(indexIndex), index.getOffset(indexIndex + 1), times);
    }

    public long getTime(TimeInfo timeInfo) {
        if (timeInfo.timeIndex >= timeInfo.maxOffset){
            timeInfo.indexIndex++;
            timeInfo.maxOffset = index.getOffset(timeInfo.indexIndex + 1);
        }
        return index.getBaseTime(timeInfo.indexIndex) | times[timeInfo.timeIndex];
    }

}