package io.openmessaging.file;

import java.io.IOException;
import java.nio.Buffer;
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

    private volatile long offset = 0;
    private AsynchronousFileChannel fileChannel;
    private BlockingQueue<ByteBuffer> buffers;
//    private ConcurrentHashMap<Integer, ByteBuffer> readBuffer = new ConcurrentHashMap<>();
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
            fileChannel = AsynchronousFileChannel.open(Paths.get(filePath),
                    StandardOpenOption.WRITE, StandardOpenOption.READ);

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
        fileChannel.write(buffer, offset, null, new CompletionHandler<Integer, Object>() {
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
        Future<Integer> future = fileChannel.write(activeBuffer, offset);
        while (!future.isDone()) {
        }
        activeBuffer.clear();
        try {
            buffers.put(activeBuffer);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ThreadLocal<ByteBuffer[]> readByteBuffers = new ThreadLocal<>();
    private ThreadLocal<Integer[]>  readBuffersStatus = new ThreadLocal<>();
    private ThreadLocal<Integer>    readBufferIndex = new ThreadLocal<>();

    public void initRead(){
        if (readByteBuffers.get() == null) {
            ByteBuffer[] readBuffers = new ByteBuffer[2];
            readBuffers[0] = buffers.poll();
            readBuffers[1] = buffers.poll();
            readByteBuffers.set(readBuffers);
            Integer[] status = new Integer[2];
            readBuffersStatus.set(status);
        }

        readByteBuffers.get()[0].clear();
        readByteBuffers.get()[1].clear();
        readBuffersStatus.get()[0] = 0;
        readBuffersStatus.get()[1] = 0;
        readBufferIndex.set(0);
    }

    public void read(long offset, boolean preRead){
        int currentReadIndex = readBufferIndex.get();
        ByteBuffer firstBuffer = readByteBuffers.get()[currentReadIndex];
        int firstStatus = readBuffersStatus.get()[currentReadIndex];
        ByteBuffer secondBuffer = readByteBuffers.get()[(currentReadIndex+1)%2];
        int secondStatus = readBuffersStatus.get()[(currentReadIndex + 1) % 2];
        switch (firstStatus){
            case 0:
                firstBuffer.clear();
                readBuffersStatus.get()[currentReadIndex] = 1;
                Future<Integer> future = fileChannel.read(firstBuffer, offset);
                while (!future.isDone()) ;
                firstBuffer.flip();
                readBuffersStatus.get()[readBufferIndex.get()] = 2;

                secondBuffer.clear();
                if (preRead) {
                    readBuffersStatus.get()[(currentReadIndex + 1) % 2] = 1;
                    fileChannel.read(secondBuffer, offset + bufferSize, readBuffersStatus.get(), new CompletionHandler<Integer, Integer[]>() {
                        @Override
                        public void completed(Integer result, Integer[] attachment) {
                            secondBuffer.flip();
                            attachment[(currentReadIndex + 1) % 2] = 2;
                        }

                        @Override
                        public void failed(Throwable throwable, Integer[] o) {
                        }
                    });
                }

                break;
            case 1:
                break;
            case 2:
                if (!firstBuffer.hasRemaining()) {
                    firstBuffer.clear();
                    readBuffersStatus.get()[currentReadIndex] = 0;
                    if (preRead) {
                        readBuffersStatus.get()[currentReadIndex] = 1;
                        fileChannel.read(firstBuffer, offset + bufferSize, readBuffersStatus.get(), new CompletionHandler<Integer, Integer[]>() {
                            @Override
                            public void completed(Integer result, Integer[] attachment) {
                                firstBuffer.flip();
                                attachment[currentReadIndex] = 2;
                            }

                            @Override
                            public void failed(Throwable throwable, Integer[] o) {
                            }
                        });
                    }
                    while (readBuffersStatus.get()[(currentReadIndex + 1) % 2] == 1){
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    readBufferIndex.set((currentReadIndex + 1) % 2);
                }
                break;
        }

    }

    public ByteBuffer getReadByteBuffer(){
        return readByteBuffers.get()[readBufferIndex.get()];
    }

}