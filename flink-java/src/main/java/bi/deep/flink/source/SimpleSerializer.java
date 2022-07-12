package bi.deep.flink.source;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;

public class SimpleSerializer<T extends Serializable> implements SimpleVersionedSerializer<T> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(T obj) {
        return SerializationUtils.serialize(obj);
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion())
            throw new IOException(String.format("Version mismatch, expected %d, actual %d", getVersion(), version));

        return SerializationUtils.deserialize(serialized);
    }
}
