package bi.deep.jdbc.parsers;

import bi.deep.jdbc.RowSchema;
import bi.deep.jdbc.visitors.ColumnVisitor;

import java.sql.ResultSet;
import java.sql.SQLException;

public class VisitorBasedParser<T> extends Parser<T> {

    private final ColumnVisitor<T> visitor;

    public VisitorBasedParser(ColumnVisitor<T> visitor) {
        this.visitor = visitor;
    }

    @Override
    public T throwableApply(ResultSet row) throws SQLException {
        visitor.open();
        visitor.visit(new RowSchema(row.getMetaData()), row);
        T result = visitor.collect();
        visitor.close();
        return result;
    }
}
