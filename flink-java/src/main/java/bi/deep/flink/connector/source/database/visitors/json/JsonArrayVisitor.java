package bi.deep.flink.connector.source.database.visitors.json;

import bi.deep.flink.connector.source.database.visitors.ArrayVisitor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class JsonArrayVisitor extends ArrayVisitor<JsonNode> {

    private final ArrayNode array;

    private final ObjectMapper om = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .build();

    public JsonArrayVisitor() {
        array = om.createArrayNode();
    }

    @Override
    public JsonNode collect() {
        return array;
    }

    @Override
    protected void visitBigDecimal(int column, BigDecimal value) {
        array.add(value);
    }

    @Override
    protected void visitBoolean(int index, Boolean value) {
        array.add(value);
    }

    @Override
    protected void visitByte(int index, Byte value) {
        array.add(value);
    }

    @Override
    protected void visitByteArray(int index, byte[] value) {
        throw new RuntimeException("Cannot parse byte array to JSON");
    }

    @Override
    protected void visitDate(int index, Date value) {
        array.add(value.toInstant().toString());
    }

    @Override
    protected void visitDouble(int index, Double value) {
        array.add(value);
    }

    @Override
    protected void visitFloat(int index, Float value) {
        array.add(value);
    }

    @Override
    protected void visitInteger(int index, Integer value) {
        array.add(value);
    }

    @Override
    protected void visitLong(int index, Long value) {
        array.add(value);
    }

    @Override
    protected void visitObject(int index, Object value) {
        throw new RuntimeException("Cannot parse object to JSON");
    }

    @Override
    protected void visitShort(int index, Short value) {
        array.add(value);
    }

    @Override
    protected void visitString(int index, String value) {
        array.add(value);
    }

    @Override
    protected void visitTime(int index, Time value) {
        array.add(value.toInstant().toString());
    }

    @Override
    protected void visitTimestamp(int index, Timestamp value) {
        array.add(value.toInstant().toString());
    }
}
