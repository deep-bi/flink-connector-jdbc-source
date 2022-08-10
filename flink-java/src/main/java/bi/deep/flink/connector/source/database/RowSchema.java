package bi.deep.flink.connector.source.database;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class RowSchema {
    private final Map<String, Integer> columnsToTypes;
    private final String[] columns;

    public RowSchema(String[] columns, Map<String, Integer> columnsToTypes) {
        this.columns = columns;
        this.columnsToTypes = columnsToTypes;
    }

    public RowSchema(ResultSetMetaData metadata) throws SQLException {
        columns = new String[metadata.getColumnCount()];
        columnsToTypes = new HashMap<>();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            columns[i] = metadata.getColumnLabel(i+1);
            columnsToTypes.put(columns[i], metadata.getColumnType(i+1));
        }
    }

    public String[] getColumns() {
        return columns;
    }

    public Map<String, Integer> getColumnsToTypes() {
        return columnsToTypes;
    }
}
