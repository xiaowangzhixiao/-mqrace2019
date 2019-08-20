package io.openmessaging;

import io.openmessaging.file.TimeIO;
import io.openmessaging.file.WriteManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {

    private static AtomicInteger idGenerator = new AtomicInteger(0);

    private ThreadLocal<Integer> id = new ThreadLocal<>();
    private ThreadLocal<WriteManager> writeManager = new ThreadLocal<>();

    private boolean writing = true;

    private TimeIO timeIO;

    @Override
    public void put(Message message) {
        if (writeManager.get() == null) {
            id.set(idGenerator.getAndIncrement());
            System.out.println("write thread:" + id.get());
            writeManager.set(WriteManager.get(id.get()));
        }

        writeManager.get().put(message);

    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (id.get() == null){
            id.set(idGenerator.getAndIncrement());
            System.out.println("read thread:" + id.get());
            // TODO: other threadLocal
        }
        if (writing){
            synchronized (this){
                if (writing){
                    // 刷盘
                    WriteManager.finishWrite();
                    writing = false;
                }
            }
        }



        return null;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return 0;
    }

}
