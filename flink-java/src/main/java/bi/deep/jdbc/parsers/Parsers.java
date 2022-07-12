package bi.deep.jdbc.parsers;

import bi.deep.jdbc.visitors.JsonStringVisitor;

public class Parsers {

    public static Parser<String> JsonString() {
        return new VisitorBasedParser<>(new JsonStringVisitor());
    }

}
