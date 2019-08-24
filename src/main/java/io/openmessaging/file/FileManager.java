package io.openmessaging.file;

import io.openmessaging.Message;
import io.openmessaging.index.BlockIndex;
import io.openmessaging.utils.Pair;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.Constant.*;

public class FileManager {
    private static final int BUFFER_LEN = 12;
    private static final int BLOCK_INDEX_SIZE = 512;
    private static final int WRITE_BUFFER_SIZE = 8 * 1024;
    private static final int BODY_BUFFER_SIZE = BODY_SIZE * WRITE_BUFFER_SIZE;
    private static final int AT_BUFFER_SIZE = AT_SIZE * WRITE_BUFFER_SIZE;

    private static final int READ_BUFFER_SIZE = 2 * 1024;
    private static final int BODY_READ_SIZE = BODY_SIZE * READ_BUFFER_SIZE;
    private static final int AT_READ_SIZE = AT_SIZE * READ_BUFFER_SIZE;

    private static final int AVG_BUFFER_SIZE = 8 * 1024;
    private static final int AT_AVG_SIZE = AT_SIZE * AVG_BUFFER_SIZE;

    private static ConcurrentHashMap<Integer, FileManager> fileManagers = new ConcurrentHashMap<>();

    private FileIO atIo;
    private FileIO bodyIo;
    private volatile int nums = 0;
    private BlockIndex blockIndex = new BlockIndex(400000);

    public static FileManager getWriteManager(int id) {
        if (!fileManagers.containsKey(id)){
            FileManager fileManager = new FileManager(id);
            fileManagers.put(id, fileManager);
            return fileManager;
        }else{
            return fileManagers.get(id);
        }
    }

    private FileManager(int id){
        atIo = new FileIO(AT_BUFFER_SIZE, BUFFER_LEN, DIR_PATH + id + "at.ali");
        bodyIo = new FileIO(BODY_BUFFER_SIZE, BUFFER_LEN, DIR_PATH + id + "body.ali");
    }

    public void put(Message message) {
        if(nums % BLOCK_INDEX_SIZE == 0){
            blockIndex.add(message.getT());
//            System.out.printf("t:%d, a:%d\n", message.getT(), message.getA());
        }
        nums++;
        ByteBuffer buffer = atIo.getActiveBuffer();
        buffer.putLong(message.getT());
        buffer.putLong(message.getA());
        atIo.write();

        buffer = bodyIo.getActiveBuffer();
        buffer.put(message.getBody());
        bodyIo.write();
    }

    public static void finishWrite(){
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
            entry.getValue().force();
        }
    }

    private void force() {
        atIo.force();
        bodyIo.force();
    }

    public static List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
//            System.out.println("read write thread " + entry.getKey());
            entry.getValue().atIo.initRead(AT_READ_SIZE);
            entry.getValue().bodyIo.initRead(BODY_READ_SIZE);
            result.addAll(entry.getValue().get(aMin,aMax,tMin,tMax));
        }

        result.sort(Comparator.comparingLong(Message::getT));
        return result;
    }

    private List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();

        int minBlock = blockIndex.searchMin(tMin);
        if (tMin > blockIndex.time[blockIndex.nums-1]){
            minBlock += 1;
        }
        int maxBlock = blockIndex.searchMax(tMax);

        int minOffset = minBlock * BLOCK_INDEX_SIZE;
        int maxOffset = maxBlock * BLOCK_INDEX_SIZE;
        if (tMax >= blockIndex.time[blockIndex.nums-1]){
            maxOffset  = this.nums;
        }
        ByteBuffer readAtBuffer = ByteBuffer.allocateDirect((maxOffset - minOffset) * AT_SIZE);
        ByteBuffer readBodyBuffer = ByteBuffer.allocateDirect((maxOffset - minOffset) * BODY_SIZE);

        long t;
        long a;
        atIo.read(readAtBuffer,(long)minOffset * (long)AT_SIZE);
        bodyIo.read(readBodyBuffer, (long) minOffset * (long) BODY_SIZE);
        int innerOffset = 0;
        do {
            t = readAtBuffer.getLong();
            a = readAtBuffer.getLong();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                Message message = new Message(a, t, new byte[34]);
                readBodyBuffer.position(innerOffset*BODY_SIZE);
                readBodyBuffer.get(message.getBody());
                result.add(message);
            }
            innerOffset ++;
        } while (t <= tMax && readAtBuffer.hasRemaining());

        return result;
    }

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax){
        long sum = 0;
        int nums = 0;
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
//            System.out.println("read avg write thread " + entry.getKey());
//            entry.getValue().atIo.initRead(AT_AVG_SIZE);
            Pair<Long, Integer> res = entry.getValue().getAvg(aMin,aMax,tMin,tMax);
            sum += res.first;
            nums += res.second;
        }
        return sum / (long)nums;
    }

    private Pair<Long, Integer> getAvg(long aMin, long aMax, long tMin, long tMax){
        long sum = 0;
        int nums = 0;

        int minBlock = blockIndex.searchMin(tMin);
        if (tMin > blockIndex.time[blockIndex.nums-1]){
            minBlock += 1;
        }
        int maxBlock = blockIndex.searchMax(tMax);

        int minOffset = minBlock * BLOCK_INDEX_SIZE;
        int maxOffset = maxBlock * BLOCK_INDEX_SIZE;
        if (tMax >= blockIndex.time[blockIndex.nums-1]){
            maxOffset  = this.nums;
        }
        ByteBuffer readBuffer = ByteBuffer.allocateDirect((maxOffset - minOffset) * AT_SIZE);

        long t;
        long a;
        atIo.read(readBuffer,(long)minOffset * (long)AT_SIZE);

        do {
            t = readBuffer.getLong();
            a = readBuffer.getLong();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                sum += a;
                nums++;
            }
        } while (t <= tMax && readBuffer.hasRemaining());

        return new Pair<>(sum,nums);
    }

}
