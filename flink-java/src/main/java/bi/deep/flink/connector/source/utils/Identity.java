package bi.deep.flink.connector.source.utils;

public class Identity<T> implements SerializableFunction<T, T> {

    @Override
    public T apply(T in) {
        return in;
    }

    public static <U> Identity<U> function() {
        return new Identity<>();
    }
}
