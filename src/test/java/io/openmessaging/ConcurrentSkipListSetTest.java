package io.openmessaging;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

class Node{
    int a;
    int b;

    public int getA() {
        return a;
    }

    public Node(int a, int b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof Node)){
            return false;
        }
        Node other = (Node) obj;
        return other.a == a && other.b == b;
    }
}
public class ConcurrentSkipListSetTest {

    public static void main(String[] args) {
        ConcurrentSkipListSet<Node> skipListSet = new ConcurrentSkipListSet<>((t1, t2) -> t1.a > t2.a ? 1 : t1 == t2 ? 0 : -1);
        Node a = new Node(1,1);
        Node b = new Node(2,1);
        skipListSet.add(a);
        skipListSet.add(b);
        System.out.println(skipListSet.size()+" "+skipListSet.first().a);
    }
}
