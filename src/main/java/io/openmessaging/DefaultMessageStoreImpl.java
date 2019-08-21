package io.openmessaging;

import io.openmessaging.file.FileManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {

    private static AtomicInteger idGenerator = new AtomicInteger(0);

    private ThreadLocal<Integer> id = new ThreadLocal<>();
    private ThreadLocal<FileManager> writeManager = new ThreadLocal<>();

    private boolean writing = true;

//    private TimeIO timeIO;

    @Override
    public void put(Message message) {
        if (writeManager.get() == null) {
            id.set(idGenerator.getAndIncrement());
            System.out.println("write thread:" + id.get());
            writeManager.set(FileManager.getWriteManager(id.get()));
        }

        writeManager.get().put(message);

    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (writing){
            synchronized (this){
                if (writing){
                    // 刷盘
                    FileManager.finishWrite();
                    writing = false;
                }
            }
        }

        return FileManager.getMessage(aMin, aMax, tMin, tMax);
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return FileManager.getAvgValue(aMin,aMax,tMin,tMax);
    }

}
