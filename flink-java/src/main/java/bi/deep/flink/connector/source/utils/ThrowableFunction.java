package bi.deep.flink.connector.source.utils;

public interface ThrowableFunction<In, Out> {

    Out throwableApply(In value) throws Throwable;

}
