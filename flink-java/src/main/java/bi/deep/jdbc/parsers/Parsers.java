package bi.deep.jdbc.parsers;

import bi.deep.jdbc.visitors.JsonStringVisitor;

public class Parsers {

    /**
     * Returns parser that represents rows as JSON string. Column names are converted to lower case.
     */
    public static Parser<String> JsonString() {
        return new VisitorBasedParser<>(new JsonStringVisitor(String::toLowerCase));
    }

}
