package io.openmessaging;
import io.openmessaging.request.RequestQueueBuffer;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class DefaultMessageStoreImpl extends MessageStore {

    private static AtomicInteger idGenerator = new AtomicInteger(0);

    private ThreadLocal<Integer> id = ThreadLocal.withInitial(()->idGenerator.getAndIncrement());

    private ThreadLocal<BlockingQueue<Message>> putBuffer = ThreadLocal.withInitial(()-> RequestQueueBuffer.getQueueBuffer(id.get(), 256));

    @Override
    public void put(Message message) {
        try {
            putBuffer.get().put(message);
            RequestQueueBuffer.put();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        System.exit(5);
        return null;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return 0;
    }

}
