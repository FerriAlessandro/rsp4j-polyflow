package shared.contentimpl.factories;

import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import shared.contentimpl.content.FirstContent;

import java.util.function.BiFunction;
import java.util.function.Function;

public class FirstContentFactory<I, W, R> implements ContentFactory<I, W, R> {

    Function<I, W> f1;
    Function<W, R> f2;
    R emptyContent;

    public FirstContentFactory(Function<I, W> f1, Function<W, R> f2,  R emptyContent){
        this.f1 = f1;
        this.f2 = f2;
        this.emptyContent = emptyContent;
    }

    @Override
    public Content<I, W, R> createEmpty() {
        throw new RuntimeException("why does this still exist?");
    }

    @Override
    public Content<I, W, R> create() {
        return new FirstContent<>(f1, f2, emptyContent);
    }
}
