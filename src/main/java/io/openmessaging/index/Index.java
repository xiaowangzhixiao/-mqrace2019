package io.openmessaging.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 索引常驻内存
 */
public class Index {
    private static Index instance = new Index();
    private List<IndexNode> index = new ArrayList<>();

    private Index() {}

    public IndexNode search(long t){
        int i =Collections.binarySearch(index,new IndexNode(t), Comparator.comparingLong(IndexNode::getT));
        return index.get(i);
    }

    public void add(IndexNode indexNode){
        index.add(indexNode);
    }

    public static Index getInstance(){
        return instance;
    }

}
