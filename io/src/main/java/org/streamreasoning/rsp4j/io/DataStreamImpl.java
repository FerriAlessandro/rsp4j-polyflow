package org.streamreasoning.rsp4j.io;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Wrapper class for {@link DataStream} providing functionality for interaction with consumers.
 *
 * @param <T> The generic type of objects in the stream.
 */
public class DataStreamImpl<T> implements DataStream<T> {
    protected List<Consumer<T>> consumers = new ArrayList<>();
    protected String stream_uri;

    public DataStreamImpl(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    @Override
    public void addConsumer(Consumer<T> c) {
        consumers.add(c);

    }


    @Override
    public void put(T t, long ts) {
        consumers.forEach(graphConsumer -> graphConsumer.notify(this, t, ts));

    }

    @Override
    public String getName() {
        return stream_uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamImpl<?> that = (DataStreamImpl<?>) o;
        return Objects.equals(stream_uri, that.stream_uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream_uri);
    }

    @Override
    public String toString() {
        return stream_uri;
    }

    public <R> DataStreamImpl<R> map(Function<? super T, ? extends R> mapper, String streamURL) {
        DataStreamImpl<R> newStream = new DataStreamImpl<>(streamURL);
        this.addConsumer((s, e, t) -> newStream.put(mapper.apply(e), t));
        return newStream;
    }
}
