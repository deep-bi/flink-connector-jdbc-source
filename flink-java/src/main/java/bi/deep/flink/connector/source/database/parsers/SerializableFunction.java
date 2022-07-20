package bi.deep.flink.connector.source.database.parsers;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
