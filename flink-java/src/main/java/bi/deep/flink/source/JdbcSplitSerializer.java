package bi.deep.flink.source;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class JdbcSplitSerializer implements SimpleVersionedSerializer<JdbcSplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(JdbcSplit obj) {
        return SerializationUtils.serialize(obj);
    }

    @Override
    public JdbcSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion())
            throw new IOException(String.format("Version mismatch, expected %d, actual %d", getVersion(), version));

        return SerializationUtils.deserialize(serialized);
    }
}
