package io.openmessaging.request;

import io.openmessaging.Message;
import io.openmessaging.file.FileManager;
import io.openmessaging.index.Index;
import io.openmessaging.index.IndexNode;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程不安全，仅供排序线程使用
 */
public class SortedRequestBuffer {

    private static final int LEN = 2;

    private static final int SKIPLIST_SIZE = 1024*4*1024;

    private static ConcurrentSkipListSet<Message>[] skipListSets = new ConcurrentSkipListSet[LEN];
    public static boolean[] writing = new boolean[LEN];
    private static int mapIndex = 0;

    private static Lock lock = new ReentrantLock();
    public static Condition completeWrite = lock.newCondition();

    private static long id = 0;

    private static Comparator<Message> messageComparator = (m1, m2) -> m1.getT() >= m2.getT() ? 1: -1;

    static {
        skipListSets[0] = new ConcurrentSkipListSet<>(messageComparator);
        skipListSets[1] = new ConcurrentSkipListSet<>(messageComparator);
    }

    public static void put(Message message){
        ConcurrentSkipListSet<Message> skipList = skipListSets[mapIndex];
        skipList.add(message);
        id++;
        if (skipList.size() == SKIPLIST_SIZE){
            IndexNode indexNode = new IndexNode(skipList.first().getT());
            writing[mapIndex] = true;
            long offset = FileManager.commit(skipList, mapIndex);
            indexNode.setOffset(offset);
            Index.getInstance().add(indexNode);
            mapIndex = (mapIndex + 1) % LEN;
            try {
                lock.lock();
                while (writing[mapIndex]){
                    completeWrite.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }



}
