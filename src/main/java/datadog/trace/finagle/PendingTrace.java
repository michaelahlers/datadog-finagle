package datadog.trace.finagle;

import com.twitter.finagle.tracing.Record;
import com.twitter.finagle.tracing.SpanId;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class PendingTrace {
    private volatile boolean completed = false;
    private final AtomicBoolean written = new AtomicBoolean(false);
    private final String serviceName;

    private long expiration;

    private ConcurrentMap<SpanId, Span> spans = new ConcurrentHashMap<>();

    public PendingTrace(String serviceName) {
        this.serviceName = serviceName;
    }

    public void addRecord(Record record) {
        Span span = spans.computeIfAbsent(record.traceId().spanId(), (key) -> new Span(record.traceId()));
        span.addRecord(record);
    }

    public boolean isComplete() {
        return completed;
    }

    public boolean canWrite() {
        return written.compareAndSet(false, true);
    }
}
