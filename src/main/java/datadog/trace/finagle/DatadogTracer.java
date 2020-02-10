package datadog.trace.finagle;

import com.google.auto.service.AutoService;
import com.twitter.finagle.tracing.Record;
import com.twitter.finagle.tracing.SpanId;
import com.twitter.finagle.tracing.TraceId;
import com.twitter.finagle.tracing.Tracer;
import datadog.trace.api.Config;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

@AutoService(Tracer.class)
public class DatadogTracer implements Tracer, Closeable {
  private static final Logger log = LoggerFactory.getLogger(DatadogTracer.class);

  public static final long FLUSH_PERIOD = TimeUnit.SECONDS.toMillis(1);

  private final Map<SpanId, PendingTrace> traces = new ConcurrentHashMap<>();

  private final ScheduledExecutorService executorService;
  private final DDApi ddApi;
  private final String serviceName;

  public DatadogTracer() {
    // Double configSampleRate = datadog.trace.api.Config.get().getTraceSamplingDefaultRate();
    this(Config.get().getServiceName(), Config.get().getAgentHost(), Config.get().getAgentPort());
  }

  public DatadogTracer(String serviceName, String agentHost, int port) {
    executorService =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "Tracer-Flush");
              thread.setDaemon(true);
              return thread;
            });
    executorService.scheduleAtFixedRate(
        this::flush, FLUSH_PERIOD, FLUSH_PERIOD, TimeUnit.MILLISECONDS);

    this.ddApi = new DDApi(agentHost, port);
    this.serviceName = serviceName;
  }

  @Override
  public void record(Record record) {
    log.info("Got record {}", record);
    PendingTrace pendingTrace =
        traces.computeIfAbsent(record.traceId().traceId(), (key) -> new PendingTrace(serviceName));
    pendingTrace.addRecord(record);

    if (pendingTrace.isComplete()) {
      traces.remove(record.traceId().traceId());
      ddApi.sendTrace(pendingTrace);
    }
  }

  @Override
  public boolean isNull() {
    return false;
  }

  @Override
  public Option<Object> sampleTrace(TraceId traceId) {
    // TODO implement sampling
    return Tracer.SomeTrue();
  }

  @Override
  public boolean isActivelyTracing(TraceId traceId) {
    return traceId.getSampled().getOrElse(() -> true);
  }

  @Override
  public void close() {
    executorService.shutdownNow();
  }

  private void flush() {
    final Iterator<Map.Entry<SpanId, PendingTrace>> iterator = traces.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<SpanId, PendingTrace> next = iterator.next();
      if (next.getValue().isComplete()) {
        iterator.remove();
        ddApi.sendTrace(next.getValue());
      }
    }
  }
}
