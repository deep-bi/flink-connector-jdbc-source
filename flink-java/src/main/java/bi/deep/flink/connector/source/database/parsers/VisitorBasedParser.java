package bi.deep.flink.connector.source.database.parsers;

import bi.deep.flink.connector.source.database.RowSchema;
import bi.deep.flink.connector.source.database.visitors.ColumnVisitor;

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
