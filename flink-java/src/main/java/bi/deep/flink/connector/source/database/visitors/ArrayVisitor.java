package bi.deep.flink.connector.source.database.visitors;

import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;

public abstract class ArrayVisitor<T> implements Serializable {

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

    public final void visit(Array array) throws SQLException {

        int index = 0;
        for (Object value : (Object[]) array.getArray()) {
            String format = "Types.%s not yet implemented";
            switch (array.getBaseType()) {
                case Types.ARRAY:
                    throw new NotImplementedException(String.format(format, "ARRAY"));
                case Types.BIGINT:
                    visitLong(index, (Long) value);
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    visitBoolean(index, (Boolean) value);
                    break;
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    visitByteArray(index, (byte[]) value);
                    break;
                case Types.BLOB:
                    throw new NotImplementedException(String.format(format, "BLOB"));
                case Types.CHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.VARCHAR:
                    visitString(index, (String) value);
                    break;
                case Types.CLOB:
                    throw new NotImplementedException(String.format(format, "CLOB"));
                case Types.DATALINK:
                    throw new NotImplementedException(String.format(format, "DATALINK"));
                case Types.DATE:
                    visitDate(index, (Date) value);
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    visitBigDecimal(index, (BigDecimal) value);
                    break;
                case Types.DISTINCT:
                    throw new NotImplementedException(String.format(format, "DISTINCT"));
                case Types.DOUBLE:
                    visitDouble(index, (Double) value);
                    break;
                case Types.FLOAT:
                    visitFloat(index, (Float) value);
                    break;
                case Types.INTEGER:
                    visitInteger(index, (Integer) value);
                    break;
                case Types.JAVA_OBJECT:
                    visitObject(index, value);
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
                    visitShort(index, (Short) value);
                    break;
                case Types.SQLXML:
                    throw new NotImplementedException(String.format(format, "SQLXML"));
                case Types.STRUCT:
                    throw new NotImplementedException(String.format(format, "STRUCT"));
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    visitTime(index, (Time) value);
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    visitTimestamp(index, (Timestamp) value);
                    break;
                case Types.TINYINT:
                    visitByte(index, (Byte) value);
                    break;
            }
        }
    }

    protected void visitByteArray(int index, byte[] value) {
    }

    protected void visitBoolean(int index, Boolean value) {
    }

    protected void visitString(int index, String value) {
    }

    protected void visitDate(int index, Date value) {
    }

    protected void visitTime(int index, Time value) {
    }

    protected void visitTimestamp(int index, Timestamp value) {
    }

    protected void visitBigDecimal(int index, BigDecimal value) {
    }

    protected void visitDouble(int index, Double value) {
    }

    protected void visitFloat(int index, Float value) {
    }

    protected void visitLong(int index, Long value) {
    }

    protected void visitObject(int index, Object value) {
    }

    protected void visitInteger(int index, Integer value) {
    }

    protected void visitShort(int index, Short value) {
    }

    protected void visitByte(int index, Byte value) {
    }
}
