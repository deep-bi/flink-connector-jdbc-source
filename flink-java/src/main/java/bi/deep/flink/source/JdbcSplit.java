package bi.deep.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;


public class JdbcSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    private final Instant timestamp = Instant.now();

    @Override
    public String splitId() {
        return timestamp.toString();
    }
}
