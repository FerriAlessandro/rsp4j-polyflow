package graph.jena.content;

import org.streamreasoning.rsp4j.api.secret.content.Content;

public class EmptyContent<I,W,R> implements Content<I, W, R> {

    long ts = System.currentTimeMillis();
    private R o;

    public EmptyContent(R o) {
        this.o = o;
    }


    @Override
    public int size() {
        return 0;
    }

    @Override
    public void add(I e) {
        throw new UnsupportedOperationException();
    }


    @Override
    public R coalesce() {
        return o;
    }
}
