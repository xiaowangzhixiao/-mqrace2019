package io.openmessaging.request;

import io.openmessaging.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestQueueBuffer {

    private static Map<Integer, BlockingQueue<Message>> requestQueueBufferMap = new HashMap<>();

    private static boolean padding = false;

    private static Lock lock = new ReentrantLock();
    private static Condition _put = lock.newCondition();

    private static boolean writing = true;

    static {
        Thread sortThread = new Thread(() -> {
            while (writing) {
                lock.lock();
                try {
                    while (!padding) {
                        _put.await();
                    }
                    padding = false;

                    for (BlockingQueue<Message> queue : requestQueueBufferMap.values()) {
                        int size = queue.size();
                        for (int i = 0; i < size; i++) {
                            Message message = queue.poll();
                            SortedRequestBuffer.put(message);
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
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

    public static void put(){
        padding = true;
        _put.signal();
    }

}
