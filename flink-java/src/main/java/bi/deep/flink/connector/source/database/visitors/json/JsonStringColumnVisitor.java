package bi.deep.flink.connector.source.database.visitors.json;

import bi.deep.flink.connector.source.utils.SerializableFunction;
import bi.deep.flink.connector.source.database.visitors.ArrayVisitor;
import bi.deep.flink.connector.source.database.visitors.ColumnVisitor;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonStringColumnVisitor extends ColumnVisitor<String> {
    private Map<String, String> object;
    private final SerializableFunction<String, String> columnDisplayName;

    public JsonStringColumnVisitor(SerializableFunction<String, String> columnDisplayName) {
        this.columnDisplayName = columnDisplayName;
    }

    @Override
    public void open() {
        object = new HashMap<>();
    }

    @Override
    protected void visitArray(String column, Array array, boolean wasNull, int sqlType) throws SQLException {
        if (wasNull) object.put(column, "null");
        else {
            ArrayVisitor<String> arrayVisitor = new JsonStringArrayVisitor();
            arrayVisitor.visit(array);
            object.put(column, arrayVisitor.collect());
        }
        array.free();
    }

    @Override
    protected void visitBigDecimal(String column, BigDecimal value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, value.toString());
    }

    @Override
    protected void visitBoolean(String column, Boolean value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, value.toString());
    }

    @Override
    protected void visitByte(String column, byte value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitByteArray(String column, byte[] value, boolean wasNull, int sqlType) {
        throw new RuntimeException("Cannot parse byte array to JSON");
    }

    @Override
    protected void visitDate(String column, Date value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    protected void visitDouble(String column, double value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitFloat(String column, float value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitInteger(String column, int value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitLong(String column, long value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitObject(String column, Object value, boolean wasNull, int sqlType) {
        throw new RuntimeException("Cannot parse object to JSON");
    }

    @Override
    protected void visitShort(String column, short value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitString(String column, String value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.format("\"%s\"", value));
    }

    @Override
    protected void visitTime(String column, Time value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    protected void visitTimestamp(String column, Timestamp value, boolean wasNull, int sqlType) {
        if (wasNull) object.put(column, "null");
        else object.put(column, String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    public String collect() {
        String json = object.entrySet().stream()
                .map(kv -> String.format("\"%s\":%s", columnDisplayName.apply(kv.getKey()), kv.getValue()))
                .collect(Collectors.joining(","));
        return String.format("{%s}", json);
    }
}
