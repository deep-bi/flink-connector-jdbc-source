package bi.deep.flink.connector.source.database.parsers;

import bi.deep.flink.connector.source.database.visitors.JsonStringVisitor;

public class Parsers {

    /**
     * Returns parser that represents rows as JSON string. Column names are converted to lower case.
     */
    public static Parser<String> JsonString() {
        return new VisitorBasedParser<>(new JsonStringVisitor(String::toLowerCase));
    }

}