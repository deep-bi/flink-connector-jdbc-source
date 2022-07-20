package bi.deep.flink.connector.source.database.parsers;

public interface ThrowableFunction<In, Out> {

    Out throwableApply(In value) throws Throwable;

}
