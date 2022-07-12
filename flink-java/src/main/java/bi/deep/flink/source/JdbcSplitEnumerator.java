package bi.deep.flink.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class JdbcSplitEnumerator implements SplitEnumerator<JdbcSplit, JdbcCheckpoint> {

    private final SplitEnumeratorContext<JdbcSplit> context;
    private final long intervalMs;
    private final long offsetMs;

    private int readerIndex = -1;

    public JdbcSplitEnumerator(SplitEnumeratorContext<JdbcSplit> context, long offsetMs, long intervalMs) {
        if (context.currentParallelism() > 1) throw new ParallelismExceededException(context.currentParallelism());
        this.context = context;
        this.offsetMs = offsetMs;
        this.intervalMs = intervalMs;
    }

    @Override
    public void start() {
        context.callAsync(this::fetchSplit, (split, error) -> {
            if (readerIndex >= 0) context.assignSplit(split, readerIndex);
        }, offsetMs, intervalMs);
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {
        // Do nothing, splits are assigned by Enumerator and Readers cannot make requests
    }

    @Override
    public void addSplitsBack(List<JdbcSplit> list, int i) {
        // Do nothing if the split fails then it is fine, it will be repeated ultimately
    }

    @Override
    public void addReader(int i) {
        // Do nothing, there should be exactly one reader
        readerIndex = i;
    }

    @Override
    public JdbcCheckpoint snapshotState(long l) {
        return new JdbcCheckpoint();
    }

    @Override
    public void close() {
    }

    private JdbcSplit fetchSplit() {
        return new JdbcSplit();
    }
}
