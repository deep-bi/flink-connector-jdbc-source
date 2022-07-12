package bi.deep.jdbc.parsers;

public interface ThrowableSupplier<Out> {

    Out throwableGet() throws Throwable;

}
