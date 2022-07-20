package bi.deep.flink.connector.source.database.visitors;

import bi.deep.flink.connector.source.database.RowSchema;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;

public abstract class ColumnVisitor<T> implements Serializable {

    /**
     * Open visitor to have clean visitor state
     */
    public void open() {
    }

    /**
     * Close visitor state
     */
    public void close() {
    }

    /**
     * Returns value accumulated during visits
     */
    public abstract T collect();

    public final void visit(RowSchema schema, ResultSet row) throws SQLException {
        for (String column : schema.getColumns()) {
            int type = schema.getColumnsToTypes().get(column);

            String format = "Types.%s not yet implemented";
            switch (type) {
                case Types.ARRAY:
                    throw new NotImplementedException(String.format(format, "ARRAY"));
                case Types.BIGINT:
                    visitLong(column, row.getLong(column), row.wasNull(), type);
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    visitBoolean(column, row.getBoolean(column), row.wasNull(), type);
                    break;
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    visitByteArray(column, row.getBytes(column), row.wasNull(), type);
                    break;
                case Types.BLOB:
                    throw new NotImplementedException(String.format(format, "BLOB"));
                case Types.CHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.VARCHAR:
                    visitString(column, row.getString(column), row.wasNull(), type);
                    break;
                case Types.CLOB:
                    throw new NotImplementedException(String.format(format, "CLOB"));
                case Types.DATALINK:
                    throw new NotImplementedException(String.format(format, "DATALINK"));
                case Types.DATE:
                    visitDate(column, row.getDate(column), row.wasNull(), type);
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    visitBigDecimal(column, row.getBigDecimal(column), row.wasNull(), type);
                    break;
                case Types.DISTINCT:
                    throw new NotImplementedException(String.format(format, "DISTINCT"));
                case Types.DOUBLE:
                    visitDouble(column, row.getDouble(column), row.wasNull(), type);
                    break;
                case Types.FLOAT:
                    visitFloat(column, row.getFloat(column), row.wasNull(), type);
                    break;
                case Types.INTEGER:
                    visitInteger(column, row.getInt(column), row.wasNull(), type);
                    break;
                case Types.JAVA_OBJECT:
                    visitObject(column, row.getObject(column), row.wasNull(), type);
                    break;
                case Types.NCLOB:
                    throw new NotImplementedException(String.format(format, "NCLOB"));
                case Types.NULL:
                    throw new NotImplementedException(String.format(format, "NULL"));
                case Types.OTHER:
                    throw new NotImplementedException(String.format(format, "OTHER"));
                case Types.REF:
                    throw new NotImplementedException(String.format(format, "REF"));
                case Types.REF_CURSOR:
                    throw new NotImplementedException(String.format(format, "REF_CURSOR"));
                case Types.ROWID:
                    throw new NotImplementedException(String.format(format, "ROWID"));
                case Types.SMALLINT:
                    visitShort(column, row.getShort(column), row.wasNull(), type);
                    break;
                case Types.SQLXML:
                    throw new NotImplementedException(String.format(format, "SQLXML"));
                case Types.STRUCT:
                    throw new NotImplementedException(String.format(format, "STRUCT"));
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    visitTime(column, row.getTime(column), row.wasNull(), type);
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    visitTimestamp(column, row.getTimestamp(column), row.wasNull(), type);
                    break;
                case Types.TINYINT:
                    visitByte(column, row.getByte(column), row.wasNull(), type);
                    break;
            }
        }
    }

    protected void visitByteArray(String column, byte[] value, boolean wasNull, int sqlType) {
    }

    protected void visitBoolean(String column, Boolean value, boolean wasNull, int sqlType) {
    }

    protected void visitString(String column, String value, boolean wasNull, int sqlType) {
    }

    protected void visitDate(String column, Date value, boolean wasNull, int sqlType) {
    }

    protected void visitTime(String column, Time value, boolean wasNull, int sqlType) {
    }

    protected void visitTimestamp(String column, Timestamp value, boolean wasNull, int sqlType) {
    }

    protected void visitBigDecimal(String column, BigDecimal value, boolean wasNull, int sqlType) {
    }

    protected void visitDouble(String column, double value, boolean wasNull, int sqlType) {
    }

    protected void visitFloat(String column, float value, boolean wasNull, int sqlType) {
    }

    protected void visitLong(String column, long value, boolean wasNull, int sqlType) {
    }

    protected void visitObject(String column, Object value, boolean wasNull, int sqlType) {
    }

    protected void visitInteger(String column, int value, boolean wasNull, int sqlType) {
    }

    protected void visitShort(String column, short value, boolean wasNull, int sqlType) {
    }

    protected void visitByte(String column, byte value, boolean wasNull, int sqlType) {
    }
}
