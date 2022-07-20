package bi.deep.flink.connector.source;

import bi.deep.flink.connector.source.checkpoint.JdbcCheckpoint;
import bi.deep.flink.connector.source.checkpoint.JdbcCheckpointSerializer;
import bi.deep.flink.connector.source.reader.JdbcReader;
import bi.deep.flink.connector.source.split.JdbcSplit;
import bi.deep.flink.connector.source.split.JdbcSplitEnumerator;
import bi.deep.flink.connector.source.split.JdbcSplitSerializer;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class JdbcSource<T> implements Source<T, JdbcSplit, JdbcCheckpoint> {

    private final JdbcSourceConfig<T> config;

    public JdbcSource(JdbcSourceConfig<T> config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, JdbcSplit> createReader(SourceReaderContext sourceReaderContext) {
        return new JdbcReader<>(config);
    }

    @Override
    public SplitEnumerator<JdbcSplit, JdbcCheckpoint> createEnumerator(SplitEnumeratorContext<JdbcSplit> splitEnumeratorContext) {
        return new JdbcSplitEnumerator(splitEnumeratorContext,
                config.getInitialDiscoveryOffset().toMillis(),
                config.getDiscoveryInterval().toMillis()
        );
    }

    @Override
    public SplitEnumerator<JdbcSplit, JdbcCheckpoint> restoreEnumerator(SplitEnumeratorContext<JdbcSplit> splitEnumeratorContext, JdbcCheckpoint jdbcCheckpoint) {
        return createEnumerator(splitEnumeratorContext);
    }

    @Override
    public SimpleVersionedSerializer<JdbcSplit> getSplitSerializer() {
        return new JdbcSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<JdbcCheckpoint> getEnumeratorCheckpointSerializer() {
        return new JdbcCheckpointSerializer();
    }
}
