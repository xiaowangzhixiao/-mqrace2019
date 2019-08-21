package io.openmessaging.file;

import io.openmessaging.Message;
import io.openmessaging.index.BlockIndex;
import io.openmessaging.utils.Pair;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.Constant.*;

public class FileManager {
    private static final int BUFFER_LEN = 24;
    private static final int BLOCK_INDEX_SIZE = 1024;
    private static final int BLOCK_SIZE = 4 * 1024;
    private static final int BODY_BUFFER_SIZE = BODY_SIZE * BLOCK_SIZE;
    private static final int AT_BUFFER_SIZE = AT_SIZE * BLOCK_SIZE;

    private static ConcurrentHashMap<Integer, FileManager> fileManagers = new ConcurrentHashMap<>();

    private FileIO atIo;
    private FileIO bodyIo;
    private volatile int nums = 0;
    private BlockIndex blockIndex = new BlockIndex(200000);

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
            entry.getValue().atIo.initRead();
            entry.getValue().bodyIo.initRead();
            result.addAll(entry.getValue().get(aMin,aMax,tMin,tMax));
        }

        result.sort(Comparator.comparingLong(Message::getT));
        return result;
    }

    private List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();

        int block = blockIndex.search(tMin);
        if (block == -1){
            block = 0;
        }

        long offset = block * BLOCK_INDEX_SIZE;
        int inOffset = 0;
        long t;
        long a;
        atIo.read(offset * AT_SIZE);
        bodyIo.read(offset * BODY_SIZE);
        ByteBuffer bodyByteBuffer = bodyIo.getReadByteBuffer();

        while (true){
            t = atIo.getReadByteBuffer().getLong();
            a = atIo.getReadByteBuffer().getLong();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax){
                bodyByteBuffer.position(inOffset*BODY_SIZE);
                Message m = new Message(a, t, new byte[34]);
                bodyByteBuffer.get(m.getBody());
                result.add(m);
            }
            if (t > tMax){
                break;
            }

            inOffset++;
            if (offset + inOffset == this.nums){
                break;
            }
            if (inOffset == BLOCK_SIZE){
                offset = offset + inOffset;
                inOffset = 0;
                atIo.read(offset * AT_SIZE);
                bodyIo.read(offset * BODY_SIZE);
                bodyByteBuffer = bodyIo.getReadByteBuffer();
            }
        }

        return result;
    }

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax){
        long sum = 0;
        int nums = 0;
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
//            System.out.println("read avg write thread " + entry.getKey());
            entry.getValue().atIo.initRead();
            Pair<Long, Integer> res = entry.getValue().getAvg(aMin,aMax,tMin,tMax);
            sum += res.first;
            nums += res.second;
        }
        return sum / (long)nums;
    }

    private Pair<Long, Integer> getAvg(long aMin, long aMax, long tMin, long tMax){
        long sum = 0;
        int nums = 0;

        int block = blockIndex.search(tMin);
        if (block == -1){
            block = 0;
        }

        long offset = block * BLOCK_INDEX_SIZE;
        int inOffset = 0;
        long t;
        long a;
        atIo.read(offset * AT_SIZE);

        while (true){
            t = atIo.getReadByteBuffer().getLong();
            a = atIo.getReadByteBuffer().getLong();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax){
                sum += a;
                nums++;
            }
            if (t > tMax){
                break;
            }

            inOffset++;
            if (offset + inOffset == this.nums){
                break;
            }
            if (inOffset == BLOCK_SIZE){
                offset = offset + inOffset;
                inOffset = 0;
                atIo.read(offset * AT_SIZE);
            }
        }

        return new Pair<>(sum,nums);
    }

}
