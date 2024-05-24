package org.streamreasoning.rsp4j.api.secret.content;


public interface Content<I, W, R> {

    int size();

    void add(I e);

    R coalesce();

    default boolean isClosed() {
        return false;
    }

}
