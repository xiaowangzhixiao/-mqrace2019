package io.openmessaging.file;

import io.openmessaging.Message;
import io.openmessaging.utils.Pair;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.Constant.*;

public class FileManager {
    private static final int BUFFER_LEN = 4;
    private static final int WRITE_BUFFER_SIZE = 8 * 1024;
    private static final int BODY_BUFFER_SIZE = BODY_SIZE * WRITE_BUFFER_SIZE;
    private static final int A_BUFFER_SIZE = A_SIZE * WRITE_BUFFER_SIZE;

    private static ConcurrentHashMap<Integer, FileManager> fileManagers = new ConcurrentHashMap<>();

    private FileIO aIo;
    private TimeIO timeIO;
    private FileIO bodyIo;
    private volatile int nums = 0;

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
        nums++;
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
        System.out.println(tMin+","+tMax+","+aMax + "," + aMin);
        List<Message> result = new ArrayList<>();

        int minIndexIndex = timeIO.getIndexIndex(tMin);
        int minTimeIndex = timeIO.getMinIndex(minIndexIndex, tMin);

        int maxTimeIndex = timeIO.getMaxIndex(tMax);

        ByteBuffer readABuffer = ByteBuffer.allocateDirect((maxTimeIndex- minTimeIndex) * A_SIZE);
        ByteBuffer readBodyBuffer = ByteBuffer.allocateDirect((maxTimeIndex - minTimeIndex) * BODY_SIZE);

        long t;
        long a;
        aIo.read(readABuffer,(long)minTimeIndex * (long)A_SIZE);
        bodyIo.read(readBodyBuffer, (long) minTimeIndex * (long) BODY_SIZE);
        int innerOffset = 0;
        TimeIO.TimeInfo timeInfo = timeIO.new TimeInfo(minIndexIndex, minTimeIndex);
        while (timeInfo.hasNext() && readABuffer.hasRemaining()){
            t = timeInfo.getNextTime();
            a = readABuffer.getLong();
            System.out.println(t + "," + a);
            if (t > tMax){
                break;
            }
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
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
        long sum = 0;
        int nums = 0;
        for (Map.Entry<Integer, FileManager> entry: fileManagers.entrySet()){
            Pair<Long, Integer> res = entry.getValue().getAvg(aMin,aMax,tMin,tMax);
            sum += res.first;
            nums += res.second;
        }
        return sum / (long)nums;
    }

    private Pair<Long, Integer> getAvg(long aMin, long aMax, long tMin, long tMax){
        long sum = 0;
        int nums = 0;
        int minIndexIndex = timeIO.getIndexIndex(tMin);
        int minTimeIndex = timeIO.getMinIndex(minIndexIndex, tMin);

        int maxTimeIndex = timeIO.getMaxIndex(tMax);

        ByteBuffer readBuffer = ByteBuffer.allocateDirect((maxTimeIndex- minTimeIndex) * A_SIZE);

        long t;
        long a;
        aIo.read(readBuffer,(long)minTimeIndex * (long)A_SIZE);
        TimeIO.TimeInfo timeInfo = timeIO.new TimeInfo(minIndexIndex, minTimeIndex);
        while (timeInfo.hasNext() && readBuffer.hasRemaining()){
            t = timeInfo.getNextTime();
            a = readBuffer.getLong();
            if (t > tMax){
                break;
            }
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                sum += a;
                nums++;
            }
        }

        return new Pair<>(sum,nums);
    }

}
