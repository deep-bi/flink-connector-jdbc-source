package bi.deep.flink.connector.source.database.parsers;

public interface ThrowableSupplier<Out> {

    Out throwableGet() throws Throwable;

}
