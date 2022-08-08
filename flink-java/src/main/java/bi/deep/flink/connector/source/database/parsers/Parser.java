package bi.deep.flink.connector.source.database.parsers;

import bi.deep.flink.connector.source.utils.Result;
import bi.deep.flink.connector.source.utils.ThrowableFunction;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.function.Function;

public abstract class Parser<T> implements Function<ResultSet, Result<T>>, ThrowableFunction<ResultSet, T>, Serializable {

    @Override
    public final Result<T> apply(ResultSet resultSet) {
        try {
            return Result.of(throwableApply(resultSet));
        } catch (Throwable ex) {
            return Result.exceptional(ex);
        }
    }
}
