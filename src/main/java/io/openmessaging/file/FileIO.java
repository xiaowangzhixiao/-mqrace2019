package io.openmessaging.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FileIo
 */
public class FileIO {

    private long offset = 0;
    private AsynchronousFileChannel writeFileChannel;
    private FileChannel readFileChannel;
    private BlockingQueue<ByteBuffer> buffers;
    private ConcurrentHashMap<Integer, ByteBuffer> readBuffer = new ConcurrentHashMap<>();
    private int bufferSize;
    private ByteBuffer activeBuffer;

    /**
     * 
     * @param bufferSize buffer块的大小
     * @param bufferNums buffer块的个数
     */
    public FileIO(int bufferSize, int bufferNums, String filePath) {
        buffers = new LinkedBlockingQueue<>(bufferNums);
        this.bufferSize = bufferSize;
        try {
            Files.createFile(Paths.get(filePath));
            writeFileChannel = AsynchronousFileChannel.open(Paths.get(filePath),
                    StandardOpenOption.WRITE);
            readFileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);

            for (int i = 0; i < bufferNums; i++) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
                buffers.put(buffer);
            }

            activeBuffer = buffers.take();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void write() {
        if (!activeBuffer.hasRemaining()) {
            writeToFile(activeBuffer);
            offset += bufferSize;

            // 取出新缓冲区
            try {
                activeBuffer = buffers.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * @return the activeBuffer
     */
    public ByteBuffer getActiveBuffer() {
        return activeBuffer;
    }

    private void writeToFile(ByteBuffer buffer) {
        buffer.flip();
        writeFileChannel.write(buffer, offset, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                buffer.clear();
                try {
                    buffers.put(buffer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
            }
        });
    }

    public void force() {
        activeBuffer.flip();
        Future<Integer> future = writeFileChannel.write(activeBuffer, offset);
        while (!future.isDone()) {
        }
        activeBuffer.clear();
        try {
            buffers.put(activeBuffer);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            writeFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}