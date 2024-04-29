package relational.content;

import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AccumulatorContent<I, W, R> implements Content<I, W, R> {


    List<W> content = new ArrayList<>();
    Function<I, W> f1;
    BiFunction<W, R, R> f2;
    public AccumulatorContent(Function<I, W> f1, BiFunction<W, R, R> f2){
        this.f1 = f1;
        this.f2 = f2;
    }


    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(I e) {
        content.add(f1.apply(e));
    }

    @Override
    public R coalesce() {
        R result = null;
        if(content.isEmpty())
            result = f2.apply(null, result);

        for(W cont : content){
            result = f2.apply(cont, result);
        }
        return result;
    }
}
