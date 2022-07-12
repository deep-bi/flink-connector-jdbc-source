package bi.deep.jdbc.parsers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

public abstract class Parser<T> implements Function<ResultSet, Result<T>>, ThrowableFunction<ResultSet, T> {

    @Override
    public final Result<T> apply(ResultSet resultSet) {
        try {
            return Result.of(throwableApply(resultSet));
        } catch (Throwable ex) {
            return Result.exceptional(ex);
        }
    }
}
