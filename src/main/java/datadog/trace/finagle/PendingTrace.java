package datadog.trace.finagle;

import com.twitter.finagle.tracing.Record;
import com.twitter.finagle.tracing.SpanId;
import datadog.trace.api.sampling.PrioritySampling;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class PendingTrace {
  private static final long TRACE_TIMEOUT = 30 * 1000;
  private volatile boolean completed = false;
  private final AtomicBoolean written = new AtomicBoolean(false);
  private final String serviceName;

  // TODO implement sampling
  private int samplingPriority = PrioritySampling.UNSET;

  private long expiration;

  private ConcurrentMap<SpanId, Span> spans = new ConcurrentHashMap<>();
  private Map<BigInteger, BigInteger> remappedSpanIds = new HashMap<>();

  public PendingTrace(String serviceName) {
    this.serviceName = serviceName;
  }

  public void addRecord(Record record) {
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
      remapSpanIdIfNecessary(span);

      for (Span oldSpan : spans.values()) {
        if (!oldSpan.isComplete()) {
          completed = false;
          return;
        }
      }

      completed = true;
    }
  }

  private void remapSpanIdIfNecessary(Span span) {
    if (Span.Kind.SERVER == span.getKind() && !BigInteger.ZERO.equals(span.getParentId())) {

      BigInteger newSpanId;
      do {
        newSpanId = new BigInteger(63, ThreadLocalRandom.current());
      } while (newSpanId.signum() == 0);

      remappedSpanIds.put(span.getSpanId(), newSpanId);
    }
  }

  public boolean isComplete() {
    return completed || System.currentTimeMillis() > expiration;
  }

  public boolean canWrite() {
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
    return new ArrayList<>(spans.values());
  }

  public Map<BigInteger, BigInteger> getRemappedSpanIds() {
    return remappedSpanIds;
  }
}
