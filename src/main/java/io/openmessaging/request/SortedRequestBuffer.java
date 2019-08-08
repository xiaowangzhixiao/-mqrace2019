package io.openmessaging.request;

import io.openmessaging.Message;
import io.openmessaging.file.FileManager;
import io.openmessaging.index.Index;
import io.openmessaging.index.IndexNode;
import io.openmessaging.list.SortedLinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程不安全，仅供排序线程使用
 */
public class SortedRequestBuffer {

    private static final int LEN = 2;

    private static final int SKIPLIST_SIZE = 128*4*1024;

    private static Map<Integer, SortedLinkedList.Node<Message>> nodeMap = new HashMap<>();

    private static SortedLinkedList<Message>[] lists = new SortedLinkedList[LEN];
    public static boolean[] writing = new boolean[LEN];
    private static int mapIndex = 0;
    private static SortedLinkedList<Message> activeList;


    public static Lock lock = new ReentrantLock();
    public static Condition completeWrite = lock.newCondition();

    static {
        lists[0] = new SortedLinkedList<>(Comparator.comparingLong(Message::getT));
        lists[1] = new SortedLinkedList<>(Comparator.comparingLong(Message::getT));
        activeList = lists[mapIndex];
    }

    public static void put(int threadId, Message message){
        SortedLinkedList.Node<Message> node;
        if (nodeMap.containsKey(threadId)){
            node = nodeMap.get(threadId);
        }else{
            node = activeList.getHead();
        }
        node = activeList.add(node, message);
        nodeMap.put(threadId, node);
        if (activeList.size() == SKIPLIST_SIZE){

            commit();

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
            activeList = lists[mapIndex];
            nodeMap.clear();
        }
    }

    public static void commit(){
        System.out.printf("start t:%d, a:%d\n",activeList.getFirst().item.getT(), activeList.getFirst().item.getA());
        System.out.printf("end t:%d, a:%d\n",activeList.getTail().item.getT(), activeList.getTail().item.getA());

        IndexNode indexNode = new IndexNode(activeList.getFirst().item.getT());
        writing[mapIndex] = true;
        long offset = FileManager.commit(activeList, mapIndex);
        indexNode.setOffset(offset);
        Index.getInstance().add(indexNode);
    }

}
