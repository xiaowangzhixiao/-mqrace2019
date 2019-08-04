package io.openmessaging.index;

public class IndexNode {
    private long t;
    private long offset;

    public IndexNode(long t, long offset) {
        this.t = t;
        this.offset = offset;
    }

    public IndexNode(long t) {
        this.t = t;
    }

    public long getT() {
        return t;
    }

    public void setT(long t) {
        this.t = t;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
