package io.openmessaging.request;

import io.openmessaging.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestQueueBuffer {

    private static Map<Integer, BlockingQueue<Message>> requestQueueBufferMap = new ConcurrentHashMap<>();

    private static boolean writing = true;

    static {
        Thread sortThread = new Thread(() -> {
            while (writing) {

                for (Map.Entry<Integer, BlockingQueue<Message>> entry : requestQueueBufferMap.entrySet()) {
                    int size = entry.getValue().size();
                    for (int i = 0; i < size; i++) {
                        Message message = null;
                        try {
                            message = entry.getValue().take();
                            SortedRequestBuffer.put(entry.getKey(), message);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

        });
        sortThread.setDaemon(true);
        sortThread.start();
    }

    public static BlockingQueue<Message> getQueueBuffer(Integer id, int size){
        BlockingQueue<Message> queue = new LinkedBlockingQueue<>(size);
        requestQueueBufferMap.put(id, queue);
        return queue; 
    }

}
