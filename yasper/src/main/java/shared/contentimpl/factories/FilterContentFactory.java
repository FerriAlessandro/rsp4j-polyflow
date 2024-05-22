package shared.contentimpl.factories;

import graph.jena.content.EmptyContent;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import shared.contentimpl.content.FilterContent;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterContentFactory<I, W, R>  implements ContentFactory<I, W, R> {

    Function<I, W> f1;
    Function<W, R> f2;
    BiFunction<R, R, R> sumR;
    R emptyContent;
    Predicate<I> predicate;

    public FilterContentFactory(Function<I, W> f1, Function<W, R> f2, BiFunction<R, R, R> sumR, R emptyContent, Predicate<I> predicate){
        this.f1 = f1;
        this.f2 = f2;
        this.sumR = sumR;
        this.predicate = predicate;
        this.emptyContent = emptyContent;
    }

    @Override
    public Content<I, W, R> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<I, W, R> create() {
        return new FilterContent<>(f1, f2, sumR, emptyContent, predicate);
    }
}
