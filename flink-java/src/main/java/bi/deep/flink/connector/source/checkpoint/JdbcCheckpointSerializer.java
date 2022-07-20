package bi.deep.flink.connector.source.checkpoint;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class JdbcCheckpointSerializer implements SimpleVersionedSerializer<JdbcCheckpoint> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(JdbcCheckpoint obj) {
        return SerializationUtils.serialize(obj);
    }

    @Override
    public JdbcCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion())
            throw new IOException(String.format("Version mismatch, expected %d, actual %d", getVersion(), version));

        return SerializationUtils.deserialize(serialized);
    }
}
