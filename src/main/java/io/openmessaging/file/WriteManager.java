package io.openmessaging.file;

import io.openmessaging.Message;
import io.openmessaging.index.BlockIndex;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.Constant.*;

public class WriteManager {
    private static final int BUFFER_LEN = 24;
    private static final int BODY_BUFFER_SIZE = BODY_SIZE * 4 * 1024;
    private static final int AT_BUFFER_SIZE = AT_SIZE * 4 * 1024;

    private static ConcurrentHashMap<Integer, WriteManager> writeManagers = new ConcurrentHashMap<>();

    private FileIO atIo;
    private FileIO bodyIo;
    private int nums = 0;
    private BlockIndex blockIndex = new BlockIndex(100000);

    public static WriteManager get(int id) {
        if (!writeManagers.containsKey(id)){
            WriteManager writeManager = new WriteManager(id);
            writeManagers.put(id, writeManager);
            return writeManager;
        }else{
            return writeManagers.get(id);
        }
    }

    private WriteManager(int id){
        atIo = new FileIO(AT_BUFFER_SIZE, BUFFER_LEN, DIR_PATH + id + "at.ali");
        bodyIo = new FileIO(BODY_BUFFER_SIZE, BUFFER_LEN, DIR_PATH + id + "body.ali");
    }

    public void put(Message message) {
        if(nums % (4*1024) == 0){
            blockIndex.add(message.getT(), nums, 4*1024);
            System.out.printf("t:%d, a:%d\n", message.getT(), message.getA());
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
        for (Map.Entry<Integer, WriteManager> entry: writeManagers.entrySet()){
            entry.getValue().force();
        }
    }

    private void force() {
        blockIndex.size[blockIndex.nums-1] = nums - blockIndex.offset[blockIndex.nums-1];
        atIo.force();
        bodyIo.force();
    }

}
