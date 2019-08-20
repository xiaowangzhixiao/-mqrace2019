package io.openmessaging.request;

import io.openmessaging.Message;
import io.openmessaging.file.FileManager;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RequestQueueBuffer {

    private static Map<Integer, BlockingQueue<Message>> requestQueueBufferMap = new ConcurrentHashMap<>();
    public static boolean writing = true;
    private static PriorityQueue<Pair<Message, BlockingQueue<Message>>> priorityQueue = new PriorityQueue<>(
            (m1, m2) -> {
                long t = m1.first.getT() - m2.first.getT();
                if (t == 0) {
                    t = m1.first.getA() - m2.first.getA();
                }
                if (t == 0) {
                    return 0;
                }
                if (t > 0) {
                    return 1;
                }
                return -1;
            });

    private static long nums = 0;

    static {
        // 排序复制线程
        Thread sortThread = new Thread(() -> {
            while (priorityQueue.size() < 12) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            while (!priorityQueue.isEmpty()) {
                Pair<Message, BlockingQueue<Message>> pair = priorityQueue.poll();
                if (nums % 100000 == 0) {
                    System.out.printf("message %d t:%d, a:%d\n", nums, pair.first.getT(), pair.first.getA());
                }
                nums++;
                FileManager.put(pair.first);
                Message next = null;
                try {
                    next = pair.second.poll(10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (next != null) {
                    priorityQueue.add(new Pair<>(next, pair.second));
                }
            }

            writing = false;

            // 完成输入
//            while (!priorityQueue.isEmpty()){
//                Pair<Message, BlockingQueue<Message>> pair = priorityQueue.poll();
//                FileManager.put(pair.first);
//            }

        });
        sortThread.setDaemon(true);
        sortThread.start();
    }

    public static BlockingQueue<Message> getQueueBuffer(Integer id, int size) {
        BlockingQueue<Message> queue = new LinkedBlockingQueue<>(size);
        requestQueueBufferMap.put(id, queue);
        return queue;
    }

    public static synchronized void putToQueue(Message message, BlockingQueue<Message> queue) {
        priorityQueue.add(new Pair<>(message, queue));
    }

}
