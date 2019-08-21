package io.openmessaging.utils;

import io.openmessaging.Message;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;

public class Sort {

    private static LinkedList<Message> merge(LinkedList<Message> messages1, LinkedList<Message> messages2){
        ListIterator<Message> it0 = messages1.listIterator();
        ListIterator<Message> it1 = messages2.listIterator();
        LinkedList<Message> result = new LinkedList<>();
        while(it0.hasNext() || it1.hasNext()) {
            if(it0.hasNext() && it1.hasNext()) {
                Message val0 = it0.next();
                Message val1 = it1.next();
                if (val0.getT() < val1.getT()) {
                    result.add(val0);
                    it1.previous();
                } else {
                    result.add(val1);
                    it0.previous();
                }
            } else if (!it0.hasNext()) {
                Message val = it1.next();
                result.add(val);
            } else {
                Message val = it0.next();
                result.add(val);
            }
        }
        messages1.clear();
        messages2.clear();
        return result;
    }

    public static List<Message> sort(LinkedList<LinkedList<Message>> messages){
        while (messages.size() > 1){
            LinkedList<Message> m1 = messages.removeFirst();
            LinkedList<Message> m2 = messages.removeFirst();
            LinkedList<Message> m3 = merge(m1, m2);
            messages.addLast(m3);
        }
        return messages.get(0);
    }

}
