package io.openmessaging.file;

import io.openmessaging.Constant;
import io.openmessaging.Message;
import io.openmessaging.list.SortedLinkedList;
import io.openmessaging.request.SortedRequestBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;

public class FileManager {
    private static final int BUFFER_LEN = 5;
    private static final int BUFFER_SIZE = 50*4*1024;

    private static long offset = 0;
    private static AsynchronousFileChannel fileChannel;
    private static BlockingQueue<ByteBuffer> writeBuffers = new LinkedBlockingQueue<>(BUFFER_LEN);

    private static ByteBuffer activeBuffer;

    static {
        try {
            Files.createFile(Paths.get("/alidata1/race2019/data/data.ali"));
            fileChannel = AsynchronousFileChannel.open(Paths.get("/alidata1/race2019/data/data.ali"),StandardOpenOption.WRITE);

            for (int i = 0; i < BUFFER_LEN; i++) {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                writeBuffers.put(byteBuffer);
            }

            activeBuffer = writeBuffers.take();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }



    private static ExecutorService executorService = Executors.newSingleThreadExecutor(r->{
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    public static long commit(SortedLinkedList<Message> sortedLinkedList, int mapIndex){
        long tmp_offset = offset;
        offset += sortedLinkedList.size() * Constant.MESSAGE_SIZE;

        CompletableFuture.supplyAsync(()->{
            // 写文件
            long _offset = tmp_offset;
            Message message;
            SortedLinkedList.Node<Message> node = sortedLinkedList.getFirst();
            while (node != null){
                message = node.item;
                activeBuffer.putLong(message.getT());
                activeBuffer.putLong(message.getA());
                activeBuffer.put(message.getBody());
                if (!activeBuffer.hasRemaining()){
                    writeToFile(_offset);
                    _offset += BUFFER_SIZE;
                    try {
                        activeBuffer = writeBuffers.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                node = node.next;
            }

            if (activeBuffer.position() != 0){
                writeToFile(_offset);
                try {
                    activeBuffer = writeBuffers.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return null;
        },executorService).thenRun(()->{
            sortedLinkedList.clear();
            SortedRequestBuffer.writing[mapIndex] = false;
            SortedRequestBuffer.lock.lock();
            SortedRequestBuffer.completeWrite.signal();
            SortedRequestBuffer.lock.unlock();
        });



        return tmp_offset;
    }

    private static void writeToFile(long _offset) {
        ByteBuffer writingBuffer = activeBuffer;
        writingBuffer.flip();
        fileChannel.write(writingBuffer, _offset, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                writingBuffer.clear();
                try {
                    writeBuffers.put(writingBuffer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void failed(Throwable exc, Object attachment) {}
        });
    }

}
