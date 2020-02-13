package datadog.trace.finagle;

import com.twitter.finagle.tracing.Record;
import com.twitter.finagle.tracing.SpanId;
import datadog.trace.api.sampling.PrioritySampling;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PendingTrace {
  private static final Logger log = LoggerFactory.getLogger(PendingTrace.class);
  private static final long TRACE_TIMEOUT = 45 * 1000;

  private final AtomicBoolean written = new AtomicBoolean(false);
  private final Map<SpanId, Span> spans = new HashMap<>();

  private volatile boolean completed = false;

  private final String serviceName;

  // TODO implement sampling
  private int samplingPriority = PrioritySampling.UNSET;
  private long expiration;

  public PendingTrace(String serviceName) {
    this.serviceName = serviceName;
  }

  public void addRecord(Record record) {
    synchronized (spans) {
      if (completed) {
        log.debug("Tried to write record after trace completion {}", record);
        return;
      }
      expiration = System.currentTimeMillis() + TRACE_TIMEOUT;

      Span span =
          spans.computeIfAbsent(
              record.traceId().spanId(),
              (key) -> {
                // Can't use longs directly because of the negative number issue with id's > 2^63
                BigInteger traceId = new BigInteger(record.traceId().traceId().toString(), 16);
                BigInteger parentId = new BigInteger(record.traceId().parentId().toString(), 16);
                BigInteger spanId = new BigInteger(record.traceId().spanId().toString(), 16);

                return new Span(this, traceId, parentId, spanId);
              });
      span.addRecord(record);

      if (span.isComplete()) {
        for (Span oldSpan : spans.values()) {
          if (!oldSpan.isComplete()) {
            completed = false;
            return;
          }
        }

        log.debug("Trace completed {}", record.traceId().traceId());
        completed = true;
      }
    }
  }

  public boolean isComplete() {
    synchronized (spans) {
      return completed || System.currentTimeMillis() > expiration;
    }
  }

  public boolean lockWrite() {
    return written.compareAndSet(false, true);
  }

  public String getServiceName() {
    return serviceName;
  }

  public Integer getSamplingPriority() {
    if (samplingPriority == PrioritySampling.UNSET) {
      return null;
    }

    return samplingPriority;
  }

  public List<Span> getSpans() {
    synchronized (spans) {
      return new ArrayList<>(spans.values());
    }
  }
}
