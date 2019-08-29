package io.openmessaging.file;

import io.openmessaging.Message;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.Constant.*;

public class FileManager {

    private static final int BUFFER_LEN = 8;
    private static final int WRITE_BUFFER_SIZE = 8 * 1024;
    private static final int BODY_BUFFER_SIZE = BODY_SIZE * WRITE_BUFFER_SIZE;
    private static final int A_BUFFER_SIZE = A_SIZE * WRITE_BUFFER_SIZE;

    private static ConcurrentHashMap<Integer, FileManager> fileManagers = new ConcurrentHashMap<>();

    private FileIO aIo;
    private TimeIO timeIO;
    private FileIO bodyIo;

    private ThreadLocal<ByteBuffer> aBuffer = new ThreadLocal<>();

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
        aIo = new FileIO(A_BUFFER_SIZE, BUFFER_LEN, DIR_PATH + id + "a.ali");
        bodyIo = new FileIO(BODY_BUFFER_SIZE, BUFFER_LEN, DIR_PATH + id + "body.ali");
        timeIO = new TimeIO();
    }

    public void put(Message message) {
        timeIO.put(message.getT());
        ByteBuffer buffer = aIo.getActiveBuffer();
        buffer.putLong(message.getA());
        aIo.write();

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
        aIo.force();
        bodyIo.force();
    }

    public static List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
            result.addAll(entry.getValue().get(aMin,aMax,tMin,tMax));
        }

        result.sort(Comparator.comparingLong(Message::getT));
        return result;
    }

    private List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();

        int minIndexIndex = timeIO.getIndexIndex(tMin);
        int minTimeIndex = timeIO.getMinIndex(minIndexIndex, tMin);

        int maxTimeIndex = timeIO.getMaxIndex(tMax);

        if (aBuffer.get() == null) {
            aBuffer.set(ByteBuffer.allocateDirect(A_SIZE * 800000));
        }

        ByteBuffer readABuffer = aBuffer.get();
        ByteBuffer readBodyBuffer = ByteBuffer.allocateDirect((maxTimeIndex - minTimeIndex) * BODY_SIZE);

        long t;
        long a;
        aIo.read(readABuffer,(long)minTimeIndex * (long)A_SIZE, (maxTimeIndex - minTimeIndex) * A_SIZE);
        bodyIo.read(readBodyBuffer, (long) minTimeIndex * (long) BODY_SIZE);
        int innerOffset = 0;
        TimeIO.TimeInfo timeInfo = timeIO.new TimeInfo(minIndexIndex, minTimeIndex);
        while (readABuffer.hasRemaining()){
            t = timeInfo.getNextTime();
            a = readABuffer.getLong();
            if (a >= aMin && a <= aMax) {
                Message message = new Message(a, t, new byte[34]);
                readBodyBuffer.position(innerOffset*BODY_SIZE);
                readBodyBuffer.get(message.getBody());
                result.add(message);
            }
            innerOffset ++;
        }

        return result;
    }

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax){
        long[] res = new long[2];
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
            entry.getValue().getAvg(aMin,aMax,tMin,tMax, res);
        }
        return res[0] / res[1];
    }

    private void  getAvg(long aMin, long aMax, long tMin, long tMax, long[] res){
        long sum = 0;
        long nums = 0;
        int minIndexIndex = timeIO.getIndexIndex(tMin);
        int minTimeIndex = timeIO.getMinIndex(minIndexIndex, tMin);

        int maxTimeIndex = timeIO.getMaxIndex(tMax);

        if (aBuffer.get() == null) {
            aBuffer.set(ByteBuffer.allocateDirect(A_SIZE * 800000));
        }

        ByteBuffer readBuffer = aBuffer.get();

        long a;
        aIo.read(readBuffer,(long)minTimeIndex * (long)A_SIZE, (maxTimeIndex- minTimeIndex) * A_SIZE);
        while (readBuffer.hasRemaining()){
            a = readBuffer.getLong();
            if (a >= aMin && a <= aMax) {
                sum += a;
                nums++;
            }
        }
        res[0] += sum;
        res[1] += nums;
    }

}
