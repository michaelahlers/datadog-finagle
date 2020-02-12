package datadog.trace.finagle;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The API pointing to a DD agent */
class DDApi implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(DDApi.class);
  private static final String DATADOG_META_LANG = "Datadog-Meta-Lang";
  private static final String DATADOG_META_LANG_VERSION = "Datadog-Meta-Lang-Version";
  private static final String DATADOG_META_LANG_INTERPRETER = "Datadog-Meta-Lang-Interpreter";
  private static final String DATADOG_META_TRACER_VERSION = "Datadog-Meta-Tracer-Version";
  private static final String X_DATADOG_TRACE_COUNT = "X-Datadog-Trace-Count";

  // TODO: determine whether these are good values
  private static final int MAX_QUEUED_TRACES = 10000;
  private static final int MAX_TRACES_PER_REQUEST = 500;

  private static final String TRACES_ENDPOINT_V4 = "/v0.4/traces";
  private static final long MILLISECONDS_BETWEEN_ERROR_LOG = TimeUnit.MINUTES.toMillis(5);

  private static final String JAVA_VERSION = System.getProperty("java.version", "unknown");
  private static final String JAVA_VM_NAME = System.getProperty("java.vm.name", "unknown");

  private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
  private final ObjectMapper jsonMapper = new ObjectMapper();

  private final String tracesEndpoint;
  private volatile long nextAllowedLogTime = 0;
  private final Thread writerThread;
  private volatile boolean running = true;

  private final BlockingQueue<PendingTrace> tracesToBeWritten =
      new ArrayBlockingQueue<>(MAX_QUEUED_TRACES);

  DDApi(final String host, final int port) {
    this.tracesEndpoint = "http://" + host + ":" + port + TRACES_ENDPOINT_V4;
    writerThread = new Thread(this::collectAndSendTraces, "dd-trace-writer");
    writerThread.setDaemon(true);
    writerThread.start();
  }

  private static HttpURLConnection getHttpURLConnection(final String endpoint) throws IOException {
    final HttpURLConnection httpCon;
    final URL url = new URL(endpoint);
    httpCon = (HttpURLConnection) url.openConnection();
    httpCon.setDoOutput(true);
    httpCon.setDoInput(true);
    httpCon.setRequestMethod("PUT");
    httpCon.setRequestProperty("Content-Type", "application/msgpack");
    httpCon.setRequestProperty(DATADOG_META_LANG, "java");
    httpCon.setRequestProperty(DATADOG_META_LANG_VERSION, JAVA_VERSION);
    httpCon.setRequestProperty(DATADOG_META_LANG_INTERPRETER, JAVA_VM_NAME);

    // TODO report lib version?
    httpCon.setRequestProperty(DATADOG_META_TRACER_VERSION, "finagle-reporter");

    return httpCon;
  }

  public void sendTrace(PendingTrace pendingTrace) {
    if (!tracesToBeWritten.offer(pendingTrace)) {
      logWithThrottling("Queue full.  Trace dropped");
    }
  }

  private void collectAndSendTraces() {
    while (running) {
      try {
        List<List<Span>> traces = new ArrayList<>();

        // After the first trace is ready, use the nonblocking version
        PendingTrace trace = tracesToBeWritten.take();
        do {
          if (trace.lockWrite()) {
            traces.add(trace.getSpans());
          }

          if (traces.size() == MAX_TRACES_PER_REQUEST) {
            break;
          }
          trace = tracesToBeWritten.poll();
        } while (trace != null);

        doSend(traces);
      } catch (InterruptedException e) {
        // do nothing
      }
    }
  }

  @Override
  public void close() {
    running = false;
    writerThread.interrupt();
  }

  private void doSend(final List<List<Span>> traces) {
    try {
      final HttpURLConnection httpCon = getHttpURLConnection(tracesEndpoint);
      httpCon.setRequestProperty(X_DATADOG_TRACE_COUNT, String.valueOf(traces.size()));

      try (final OutputStream out = httpCon.getOutputStream()) {
        objectMapper.writeValue(out, traces);
      }

      if (log.isDebugEnabled()) {
        log.debug("Sending traces {}", jsonMapper.writeValueAsString(traces));
      }

      skipAllContent(httpCon);

      final int responseCode = httpCon.getResponseCode();
      if (responseCode == 200) {
        log.trace("Succesfully sent {} traces to the DD agent.", traces.size());
      } else {
        logWithThrottling(
            "Error while sending {} traces to the DD agent. Status: {}, ResponseMessage: {}",
            traces.size(),
            responseCode,
            httpCon.getResponseMessage());
      }
    } catch (final IOException e) {
      logWithThrottling("Error while sending {} traces to the DD agent.", traces.size(), e);
    }
  }

  /* Ensure we read the full response. Borrowed from https://github.com/openzipkin/zipkin-reporter-java/blob/2eb169e/urlconnection/src/main/java/zipkin2/reporter/urlconnection/URLConnectionSender.java#L231-L252 */
  private void skipAllContent(final HttpURLConnection connection) throws IOException {
    final InputStream in = connection.getInputStream();

    final IOException thrown = skipAndSuppress(in);
    if (thrown == null) {
      return;
    }
    final InputStream err = connection.getErrorStream();
    if (err != null) {
      skipAndSuppress(err); // null is possible, if the connection was dropped
    }
    throw thrown;
  }

  private IOException skipAndSuppress(final InputStream in) {
    try {
      while (in.read() != -1) {
        // skip
      }
      return null;
    } catch (final IOException e) {
      return e;
    } finally {
      try {
        in.close();
      } catch (final IOException suppressed) {
      }
    }
  }

  private void logWithThrottling(String message, Object... args) {
    if (log.isTraceEnabled()) {
      log.trace(message, args);
    } else if (nextAllowedLogTime < System.currentTimeMillis()) {
      nextAllowedLogTime = System.currentTimeMillis() + MILLISECONDS_BETWEEN_ERROR_LOG;
      log.warn(
          message
              + " Going silent for "
              + TimeUnit.MILLISECONDS.toMinutes(MILLISECONDS_BETWEEN_ERROR_LOG)
              + " seconds",
          args);
    }
  }
}
