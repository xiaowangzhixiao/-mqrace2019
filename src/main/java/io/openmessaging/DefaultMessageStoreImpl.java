package io.openmessaging;

import io.openmessaging.file.FileManager;
import io.openmessaging.request.RequestQueueBuffer;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {

    private static AtomicInteger idGenerator = new AtomicInteger(0);

    private ThreadLocal<Integer> id = new ThreadLocal<>();

    private ThreadLocal<BlockingQueue<Message>> putBuffer = new ThreadLocal<>();

    @Override
    public void put(Message message) {
        if (putBuffer.get() == null) {
            id.set(idGenerator.getAndIncrement());
            putBuffer.set(RequestQueueBuffer.getQueueBuffer(id.get(), 16384));
            RequestQueueBuffer.putToQueue(message, putBuffer.get());
        } else {

            try {
                while (!putBuffer.get().offer(message, 10, TimeUnit.NANOSECONDS))
                    ;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (RequestQueueBuffer.writing) {
            synchronized (this) {
                if (RequestQueueBuffer.writing) {
                    while (RequestQueueBuffer.writing) {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    FileManager.finishWrite();
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
