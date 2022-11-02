package bi.deep.flink.connector.source.database.visitors.json;

import bi.deep.flink.connector.source.database.visitors.ArrayVisitor;
import bi.deep.flink.connector.source.database.visitors.ColumnVisitor;
import bi.deep.flink.connector.source.utils.SerializableFunction;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.sql.*;

public class JsonStringColumnVisitor extends ColumnVisitor<String> {
    private ObjectNode object;
    private final ObjectMapper om = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .build();
    private final SerializableFunction<String, String> columnDisplayName;

    public JsonStringColumnVisitor(SerializableFunction<String, String> columnDisplayName) {
        this.columnDisplayName = columnDisplayName;
    }

    @Override
    public void open() {
        object = om.createObjectNode();
    }

    @Override
    protected void visitArray(String column, Array array, boolean wasNull, int sqlType) throws SQLException {
        if (array == null || wasNull) object.putPOJO(column, null);
        else {
            ArrayVisitor<JsonNode> arrayVisitor = new JsonArrayVisitor();
            arrayVisitor.visit(array);
            object.set(columnDisplayName.apply(column), arrayVisitor.collect());
        }

        if (array != null) array.free();
    }

    @Override
    protected void visitBigDecimal(String column, BigDecimal value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitBoolean(String column, Boolean value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitByte(String column, byte value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitByteArray(String column, byte[] value, boolean wasNull, int sqlType) {
        throw new RuntimeException("Cannot parse byte array to JSON");
    }

    @Override
    protected void visitDate(String column, Date value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value.toInstant().toString());
    }

    @Override
    protected void visitDouble(String column, double value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitFloat(String column, float value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitInteger(String column, int value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitLong(String column, long value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitObject(String column, Object value, boolean wasNull, int sqlType) {
        object.putPOJO(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitShort(String column, short value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitString(String column, String value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value);
    }

    @Override
    protected void visitTime(String column, Time value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value.toInstant().toString());
    }

    @Override
    protected void visitTimestamp(String column, Timestamp value, boolean wasNull, int sqlType) {
        object.put(columnDisplayName.apply(column), value.toInstant().toString());
    }

    @Override
    public String collect() {
        return object.toString();
    }
}
