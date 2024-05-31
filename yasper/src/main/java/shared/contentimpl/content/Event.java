package shared.contentimpl.content;

public interface Event<T> {

    long timestamp();

    T get();
}
