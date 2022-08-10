package bi.deep.flink.connector.source.database.visitors.json;

import bi.deep.flink.connector.source.database.visitors.ArrayVisitor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

public class JsonStringArrayVisitor extends ArrayVisitor<String> {

    private final List<String> array;

    public JsonStringArrayVisitor() {
        array = new LinkedList<>();
    }

    @Override
    public String collect() {
        return "[" + String.join(",", array) + "]";
    }

    @Override
    protected void visitBigDecimal(int column, BigDecimal value) {
        if (value == null) array.add("null");
        else array.add(value.toString());
    }

    @Override
    protected void visitBoolean(int index, Boolean value) {
        if (value == null) array.add("null");
        else array.add(value.toString());
    }

    @Override
    protected void visitByte(int index, Byte value) {
        if (value == null) array.add("null");
        else array.add(String.valueOf(value));
    }

    @Override
    protected void visitByteArray(int index, byte[] value) {
        throw new RuntimeException("Cannot parse byte array to JSON");
    }

    @Override
    protected void visitDate(int index, Date value) {
        if (value == null) array.add("null");
        else array.add(String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    protected void visitDouble(int index, Double value) {
        if (value == null) array.add("null");
        else array.add(String.valueOf(value));
    }

    @Override
    protected void visitFloat(int index, Float value) {
        if (value == null) array.add("null");
        else array.add(String.valueOf(value));
    }

    @Override
    protected void visitInteger(int index, Integer value) {
        if (value == null) array.add("null");
        else array.add(String.valueOf(value));
    }

    @Override
    protected void visitLong(int index, Long value) {
        if (value == null) array.add("null");
        else array.add(String.valueOf(value));
    }

    @Override
    protected void visitObject(int index, Object value) {
        throw new RuntimeException("Cannot parse object to JSON");
    }

    @Override
    protected void visitShort(int index, Short value) {
        if (value == null) array.add("null");
        else array.add(String.valueOf(value));
    }

    @Override
    protected void visitString(int index, String value) {
        if (value == null) array.add("null");
        else array.add(String.format("\"%s\"", value));
    }

    @Override
    protected void visitTime(int index, Time value) {
        if (value == null) array.add("null");
        else array.add(String.format("\"%s\"", value.toInstant().toString()));
    }

    @Override
    protected void visitTimestamp(int index, Timestamp value) {
        if (value == null) array.add("null");
        else array.add(String.format("\"%s\"", value.toInstant().toString()));
    }
}
