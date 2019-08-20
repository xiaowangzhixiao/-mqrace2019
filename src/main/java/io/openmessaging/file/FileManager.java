package io.openmessaging.file;

import io.openmessaging.Message;

import static io.openmessaging.Constant.A_SIZE;
import static io.openmessaging.Constant.BODY_SIZE;

public class FileManager {
    private static final int BUFFER_LEN = 5;
    private static final int BODY_BUFFER_SIZE = BODY_SIZE * 4 * 1024;
    private static final int A_BUFFER_SIZE = A_SIZE * 4 * 1024;

    private static TimeIO timeIo = new TimeIO();
    private static FileIo aIo = new FileIo(A_BUFFER_SIZE, BUFFER_LEN, "/alidata1/race2019/data/a.ali");
    private static FileIo bodyIo = new FileIo(BODY_BUFFER_SIZE, BUFFER_LEN, "/alidata1/race2019/data/body.ali");

    public static void put(Message message) {
        aIo.getActiveBuffer().putLong(message.getA());
        bodyIo.getActiveBuffer().put(message.getBody());
        aIo.write();
        bodyIo.write();
    }

    public static void finishWrite() {
        aIo.force();
        bodyIo.force();
    }

}
