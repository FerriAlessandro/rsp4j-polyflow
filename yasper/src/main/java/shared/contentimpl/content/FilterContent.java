package shared.contentimpl.content;

import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterContent <I, W, R> implements Content<I, W, R> {

    List<W> content = new ArrayList<>();

    Function<I, W> f1;
    Function<W, R> f2;
    BiFunction<R, R, R> sumR;
    R emptyContent;
    Predicate<I> predicate;

    public FilterContent(Function<I, W> f1, Function<W, R> f2, BiFunction<R, R, R> sumR, R emptyContent, Predicate<I> predicate){
        this.f1 = f1;
        this.f2 = f2;
        this.sumR = sumR;
        this.emptyContent = emptyContent;
        this.predicate = predicate;
    }


    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(I e) {
        if(predicate.test(e))
            content.add(f1.apply(e));
        else System.out.print("Not adding element "+e.toString()+" to window content\n");

    }

    @Override
    public R coalesce() {
        R result = content.stream().map(f2).reduce(emptyContent,  (x, y) -> sumR.apply(x,y));
        return result;
    }
}
