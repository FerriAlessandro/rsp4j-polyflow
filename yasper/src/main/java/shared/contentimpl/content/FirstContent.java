package shared.contentimpl.content;

import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class FirstContent<I, W, R> implements Content<I, W, R> {

    W content;
    Function<I, W> f1;
    Function<W, R> f2;
    R emptyContent;
    public FirstContent(Function<I, W> f1, Function< W, R> f2, R emptyContent){
        this.f1 = f1;
        this.f2 = f2;
        this.emptyContent = emptyContent;
    }


    @Override
    public int size() {
        return content!=null? 1: 0;
    }

    @Override
    public void add(I e) {
        if(content == null){
            content = f1.apply(e);
        }

    }

    @Override
    public R coalesce() {
        return content == null? emptyContent : f2.apply(content);
    }
}
