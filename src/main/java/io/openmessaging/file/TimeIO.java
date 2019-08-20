package io.openmessaging.file;

import io.openmessaging.index.TimeIndex;

/**
 * TimeIO
 */
public class TimeIO {
    private TimeIndex index = new TimeIndex(100000000);
    private byte[] times = new byte[Integer.MAX_VALUE / 2];
    private int nums = 0;
    private long baseTime = 0;

    public void put(long time) {
        if (time < baseTime || time >= baseTime + 15){
            baseTime = time & 0x0f;
            index.put(baseTime, nums);
        }
        byte b = this.times[nums/2];
        if ((nums & 0x01) == 0){
            this.times[nums/2] = (byte) (b | (byte) ((time&0x0fL) << 4));
        }else{
            this.times[nums/2] = (byte) (b | (byte) (time&0x0fL) );
        }
        nums++;

    }

    public int getIndex(long time) {
        return index.search(time);
    }

    public long getTime(int indexIndex, int timeIndex) {
        int offset = index.getOffset(indexIndex);
        if ((offset&0x01) == 0){
            return index.getBaseTime(offset) + ((times[timeIndex]&0xf0) >>> 4 );
        }else {
            return index.getBaseTime(offset) + (times[timeIndex]&0x0f);
        }
    }

}