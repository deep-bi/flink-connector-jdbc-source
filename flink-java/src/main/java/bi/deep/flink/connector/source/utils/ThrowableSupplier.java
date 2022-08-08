package bi.deep.flink.connector.source.utils;

public interface ThrowableSupplier<Out> {

    Out throwableGet() throws Throwable;

}
