package bi.deep.flink.connector.source.database.parsers;

import bi.deep.flink.connector.source.database.visitors.json.JsonStringColumnVisitor;
import bi.deep.flink.connector.source.utils.Identity;
import bi.deep.flink.connector.source.utils.SerializableFunction;

public class Parsers {

    /**
     * Returns parser that represents rows as JSON string. Column names are converted to lower case.
     */
    public static Parser<String> JsonString() {
        return new VisitorBasedParser<>(new JsonStringColumnVisitor(Identity.function()));
    }

    public static Parser<String> JsonString(SerializableFunction<String, String> columnDisplayName) {
        return new VisitorBasedParser<>(new JsonStringColumnVisitor(columnDisplayName));
    }
}
