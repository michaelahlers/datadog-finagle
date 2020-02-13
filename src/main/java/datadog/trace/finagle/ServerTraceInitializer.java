package datadog.trace.finagle;

import com.twitter.finagle.Filter;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.Stack;
import com.twitter.finagle.http.HttpTracing.Header$;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.TraceInfo;
import com.twitter.finagle.param.Tracer;
import com.twitter.finagle.tracing.Flags;
import com.twitter.finagle.tracing.SpanId;
import com.twitter.finagle.tracing.Trace;
import com.twitter.finagle.tracing.TraceId;
import com.twitter.finagle.tracing.TraceId128;
import com.twitter.finagle.tracing.TraceInitializerFilter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import scala.Option;

/** Extracts trace ids from incoming requests and applies it to the local trace */
public class ServerTraceInitializer<Req extends Request, Rep>
    extends Stack.Module1<Tracer, ServiceFactory<Req, Rep>> {
  public ServerTraceInitializer() {
    super(Tracer.param());
  }

  @Override
  public Stack.Role role() {
    return TraceInitializerFilter.role();
  }

  @Override
  public String description() {
    return "Extracts trace ids from incoming requests";
  }

  @Override
  public ServiceFactory<Req, Rep> make(Tracer tracer, ServiceFactory<Req, Rep> next) {

    Filter<Req, Rep, Req, Rep> filter =
        Filter.mk(
            (req, service) ->
                Trace.letTracer(
                    Tracer.unapply(tracer).get(),
                    () -> letTraceIdFromRequestHeaders(req, () -> service.apply(req))));

    return filter.andThen(next);
  }

  // This is a reimplementation of com.twitter.finagle.http.TraceInfo that creates a new span
  // id for the server span
  private <R> R letTraceIdFromRequestHeaders(Request request, Supplier<R> f) {
    TraceInfo.convertB3Trace(request);

    TraceId traceId = null;

    if (Header$.MODULE$.hasAllRequired(request.headerMap())) {
      // Assign spanId to parent span id!!
      Option<SpanId> parentSpanId =
          SpanId.fromString((String) request.headerMap().apply(Header$.MODULE$.SpanId()));

      if (parentSpanId.isDefined()) {
        TraceId128 trace128Bit =
            TraceId128.apply((String) request.headerMap().apply(Header$.MODULE$.TraceId()));

        String maybeSampled = request.headerMap().getOrNull(Header$.MODULE$.Sampled());
        boolean sampled = "1".equals(maybeSampled) || Boolean.parseBoolean(maybeSampled);

        Flags flags = TraceInfo.getFlags(request);

        traceId =
            TraceId.apply(
                trace128Bit.low(),
                parentSpanId,
                newSpanId(),
                Option.apply(sampled),
                flags,
                trace128Bit.high(),
                false);
      } else {
        traceId = Trace.nextId();
      }

    } else if (request.headerMap().contains(Header$.MODULE$.Flags())) {
      traceId = traceIdWithFlag(Trace.nextId(), TraceInfo.getFlags(request));
    } else {
      traceId = Trace.nextId();
    }

    Header$.MODULE$.All().foreach((h) -> request.headerMap().remove(h));

    if (traceId != null) {
      return Trace.letId(
          traceId,
          false,
          () -> {
            TraceInfo.traceRpc(request);
            return f.get();
          });
    } else {
      TraceInfo.traceRpc(request);
      return f.get();
    }
  }

  private static TraceId traceIdWithFlag(TraceId orig, Flags flags) {
    return new TraceId(
        orig._traceId(),
        orig._parentId(),
        orig.spanId(),
        orig._sampled(),
        flags,
        orig.traceIdHigh(),
        orig.terminal());
  }

  private static SpanId newSpanId() {
    long nextId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    return SpanId.apply(nextId);
  }
}
