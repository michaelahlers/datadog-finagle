package datadog.trace.finagle;

import java.math.BigInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

import java.io.Closeable;
import java.io.Flushable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Collect spans from Zipkin and group them into traces so they can be reported together. Traces are
 * reported to the agent after a span is reported that appears to be the root, or after a
 * TIMEOUT_DELAY period. A span is assumed to be a root if it has a Span.Kind of either SERVER or
 * CONSUMER.
 *
 * <p>This implementation groups spans into traces using an unbounded ConcurrentHashMap.
 * "Incomplete" traces are flushed after 30 seconds, but only 1 second if a root span is reported.
 * This means that spikes of traffic might cause unbounded growth of the contained
 * ConcurrentHashMap.
 */
public class DatadogReporter implements Reporter<Span>, Flushable, Closeable {
    public static final long TIMEOUT_DELAY = TimeUnit.SECONDS.toNanos(30);
    public static final long COMPLETION_DELAY = TimeUnit.SECONDS.toNanos(1);
    public static final long FLUSH_DELAY = TimeUnit.SECONDS.toMillis(1);

    private final Map<String, PendingTrace> pendingTraces = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final DDApi ddApi;
    private final Thread flushingThread;

    /** Report traces to the Datadog Agent at the default location (localhost:8126). */
    public DatadogReporter() {
        this(DDApi.DEFAULT_HOSTNAME, DDApi.DEFAULT_PORT);
    }

    /**
     * Report traces to the configured Datadog Agent.
     *
     * @param host (See DDApi.DEFAULT_HOSTNAME)
     * @param port (See DDApi.DEFAULT_PORT)
     */
    public DatadogReporter(final String host, final int port) {
        ddApi = new DDApi(host, port);

        flushingThread = new Thread(this::flushPeriodically, "zipkin-datadog-flusher");
        flushingThread.setDaemon(true);
        flushingThread.start();
    }

    @Override
    public void report(final Span span) {
        final PendingTrace trace =
                pendingTraces.computeIfAbsent(span.traceId(), (id) -> new PendingTrace());

        trace.addSpan(span);

        /* If the span kind is server or consumer, we assume it is the root of the trace.
         * That implies all span children have likely already been reported and can be
         * flushed in the next cycle, though in some async cases, this might not be the case.
         */
        if (span.kind() == Span.Kind.SERVER
                || span.kind() == Span.Kind.CONSUMER
                || span.parentId() == null) {
            trace.expiration = nanoTime() + COMPLETION_DELAY;
        } else {
            trace.expiration = nanoTime() + TIMEOUT_DELAY;
        }
    }

    @Override
    public void flush() {
        final long currentTime = nanoTime();
        final Iterator<Map.Entry<String, PendingTrace>> iterator = pendingTraces.entrySet().iterator();

        final List<List<DDMappingSpan>> reportingTraces = new ArrayList<>();

        while (iterator.hasNext()) {
            final Map.Entry<String, PendingTrace> next = iterator.next();
            if (currentTime > next.getValue().expiration) {
                iterator.remove();

                if (!next.getValue().spans.isEmpty()) {
                    reportingTraces.add(next.getValue().spans);
                }
            }
        }
        if (!reportingTraces.isEmpty()) {
            ddApi.sendTraces(reportingTraces);
        }
    }

    private void flushPeriodically() {
        while (running.get()) {
            try {
                flush();
                Thread.sleep(FLUSH_DELAY);
            } catch (final InterruptedException e) {
            }
        }
    }

    @Override
    public void close() {
        running.set(false);

        flushingThread.interrupt();
        try {
            flushingThread.join();
        } catch (final InterruptedException e) {
        }
    }

    protected static long nanoTime() {
        return System.nanoTime();
    }

    private static class PendingTrace {
        private static final Logger log = LoggerFactory.getLogger(PendingTrace.class);

        // Zipkin uses the same span id for client<->server spans
        // This mapping overwrites that
        private final Map<String, BigInteger> spanMapping = new ConcurrentHashMap<>();

        private final List<DDMappingSpan> spans = new CopyOnWriteArrayList<>();
        public volatile long expiration = nanoTime() + TIMEOUT_DELAY;

        public void addSpan(final Span span) {
            // skip "flush" spans
            if ("unknown".equals(span.name()) && span.durationAsLong() == 0L) {
                return;
            }

            if (Span.Kind.SERVER.equals(span.kind()) && span.parentId() != null) {
                spanMapping.put(span.id(), generateSpanId());
            }
            final DDMappingSpan ddMappingSpan = new DDMappingSpan(span, spanMapping);

            spans.add(ddMappingSpan);
        }

        private BigInteger generateSpanId() {
            BigInteger value;
            do {
                value = new BigInteger(63, ThreadLocalRandom.current());
            } while (value.signum() == 0);

            return value;
        }
    }
}