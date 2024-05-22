package shared.contentimpl.content;

import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class AccumulatorContent<I, W, R> implements Content<I, W, R> {


    List<W> content = new ArrayList<>();
    Function<I, W> f1;
    Function<W, R> f2;
    BiFunction<R,R, R> sumR;
    R emptyContent;
    public AccumulatorContent(Function<I, W> f1, Function< W, R> f2, BiFunction<R,R,R> sumR, R emptyContent){
        this.f1 = f1;
        this.f2 = f2;
        this.sumR=sumR;
        this.emptyContent = emptyContent;
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

        return content.stream().map(f2).reduce(emptyContent,  (x, y) -> sumR.apply(x,y));
    }
}
