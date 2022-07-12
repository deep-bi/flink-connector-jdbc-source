package bi.deep.jdbc.visitors;

import bi.deep.jdbc.parsers.SerializableFunction;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JsonStringVisitor extends ColumnVisitor<String> {
    private Map<String, String> object;
    private final SerializableFunction<String, String> columnDisplayName;

    public JsonStringVisitor(SerializableFunction<String, String> columnDisplayName) {
        this.columnDisplayName = columnDisplayName;
    }

    @Override
    public void open() {
        object = new HashMap<>();
    }

    @Override
    protected void visitBigDecimal(String column, BigDecimal value, int sqlType) {
        object.put(column, value.toString());
    }

    @Override
    protected void visitBoolean(String column, Boolean value, int sqlType) {
        object.put(column, value.toString());
    }

    @Override
    protected void visitByte(String column, byte value, int sqlType) {
        object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitByteArray(String column, byte[] value, int sqlType) {
        throw new RuntimeException("Cannot parse byte array to JSON");
    }

    @Override
    protected void visitDate(String column, Date value, int sqlType) {
        object.put(column, String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    protected void visitDouble(String column, double value, int sqlType) {
        object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitFloat(String column, float value, int sqlType) {
        object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitInteger(String column, int value, int sqlType) {
        object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitLong(String column, long value, int sqlType) {
        object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitObject(String column, Object value, int sqlType) {
        throw new RuntimeException("Cannot parse object to JSON");
    }

    @Override
    protected void visitShort(String column, short value, int sqlType) {
        object.put(column, String.valueOf(value));
    }

    @Override
    protected void visitString(String column, String value, int sqlType) {
        object.put(column, String.format("\"%s\"", value));
    }

    @Override
    protected void visitTime(String column, Time value, int sqlType) {
        object.put(column, String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    protected void visitTimestamp(String column, Timestamp value, int sqlType) {
        object.put(column, String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    public String collect() {
        String json = object.entrySet().stream()
                .map(kv -> String.format("\"%s\":%s", columnDisplayName.apply(kv.getKey()), kv.getValue()))
                .collect(Collectors.joining(","));
        return String.format("{%s}", json);
    }
}
