package bi.deep.jdbc.parsers;


import java.util.function.Supplier;

/**
 * Result holds either result value or exception.
 */
public class Result<T> {

    private final T value;
    private final Throwable throwable;

    private Result(T value, Throwable throwable) {
        this.value = value;
        this.throwable = throwable;
    }

    public static <T> Result<T> exceptional(Throwable throwable) {
        return new Result<>(null, throwable);
    }

    public static <T> Result<T> of(T value) {
        return new Result<>(value, null);
    }

    /**
     * Attempt to read supplier that may throw an exception
     */
    public static <T> Result<T> attempt(Supplier<T> fn) {
        try {
            return Result.of(fn.get());
        } catch (Throwable throwable) {
            return Result.exceptional(throwable);
        }
    }

    public T get() {
        if (throwable != null) throw new RuntimeException("Result contained exception:", throwable);
        return value;
    }

    public T getOrElse(Supplier<T> other) {
        if (throwable != null) return other.get();
        else return value;
    }

    public boolean hasValue() {
        return throwable == null;
    }
}
