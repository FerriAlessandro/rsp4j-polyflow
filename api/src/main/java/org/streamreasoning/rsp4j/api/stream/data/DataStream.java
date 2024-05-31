package org.streamreasoning.rsp4j.api.stream.data;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;

import java.util.function.BiFunction;

/**
 * Created by riccardo on 10/07/2017.
 */

//TODO wrap schema for RDFUtils stream?
public interface DataStream<E> {

    void addConsumer(Consumer<E> windowAssigner);

    void put(E e, long ts);

    String getName();

    default <R> DataStream<R> map(DataStream<R> s, BiFunction<Long, ? super E, ? extends R> mapper) {
        this.addConsumer((inputStream, element, timestamp) -> s.put(mapper.apply(timestamp, element), timestamp));
        return s;
    }

}
