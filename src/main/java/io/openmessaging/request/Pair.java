package io.openmessaging.request;

/**
 * Pair
 */
public class Pair<F, S> {
    public F first;
    public S second;
    public Pair(F first, S second){
        this.first = first;
        this.second = second;
    }
}