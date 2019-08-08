package io.openmessaging.list;

import io.openmessaging.Message;

import java.util.Comparator;
import java.util.function.Consumer;

public class SortedLinkedList<E> {

    private int size = 0;
    private Comparator<E> comparator;
    private Node<E> head;
    private Node<E> tail;

    public static class Node<E> {
        public E item;
        public SortedLinkedList.Node<E> next;

        Node(E item, SortedLinkedList.Node<E> next) {
            this.item = item;
            this.next = next;
        }
    }

    private SortedLinkedList(){
        head = new Node<>(null, null);
        tail = head;
    }

    public SortedLinkedList(Comparator<E> comparator){
        this();
        this.comparator = comparator;
    }

    public void add(E item){
        tail.next = new Node<>(item, null);
        tail = tail.next;
        size++;
    }

    public Node<E> add(Node<E> pre, E item){
        Node<E> node = new Node<>(item, pre.next);
        if (pre.next == null){
            tail = node;
        }
        pre.next = node;

        size++;
        return pre.next;
    }

    public Node<E> searchAndAdd(Node<E> before, E item){
        while (before.next != null){
            if (comparator.compare(before.next.item,item) > 0){
                break;
            }
            before = before.next;
        }
        Node<E> node =  new Node<>(item, before.next);
        if (before.next == null){
            tail = node;
        }
        before.next = node;
        size++;
        return before.next;
    }

    public void visit(Consumer<E> consumer){
        Node<E> node = head.next;
        while (node != null){
            consumer.accept(node.item);
            node = node.next;
        }
    }

    public void clear(){
        Node next;
        for(Node node = this.head; node != null; node = next) {
            next = node.next;
            node.item = null;
            node.next = null;
        }
        this.size = 0;
    }

    public void visitAndClear(Consumer<E> consumer){
        Node<E> node = head.next;
        Node<E> pre = head;
        while (node != null){
            pre.item = null;
            pre.next = null;
            consumer.accept(node.item);
            pre = node;
            node = node.next;
        }
        pre.item = null;
        this.size = 0;
    }

    public int size() {
        return size;
    }

    public Node<E> getHead() {
        return head;
    }

    public Node<E> getFirst() {
        return head.next;
    }

    public Node<E> getTail() {
        return tail;
    }
}


