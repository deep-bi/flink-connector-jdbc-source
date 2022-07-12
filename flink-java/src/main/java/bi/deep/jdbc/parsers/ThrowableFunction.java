package bi.deep.jdbc.parsers;

public interface ThrowableFunction<In, Out> {

    Out throwableApply(In value) throws Throwable;

}
