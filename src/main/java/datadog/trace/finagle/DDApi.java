package datadog.trace.finagle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The API pointing to a DD agent
 */
class DDApi {
    private static final Logger log = LoggerFactory.getLogger(DDApi.class);
    private static final String DATADOG_META_LANG = "Datadog-Meta-Lang";
    private static final String DATADOG_META_LANG_VERSION = "Datadog-Meta-Lang-Version";
    private static final String DATADOG_META_LANG_INTERPRETER = "Datadog-Meta-Lang-Interpreter";
    private static final String DATADOG_META_TRACER_VERSION = "Datadog-Meta-Tracer-Version";
    private static final String X_DATADOG_TRACE_COUNT = "X-Datadog-Trace-Count";

    private static final String TRACES_ENDPOINT_V4 = "/v0.4/traces";
    private static final long MILLISECONDS_BETWEEN_ERROR_LOG = TimeUnit.MINUTES.toMillis(5);

    private static final String JAVA_VERSION = System.getProperty("java.version", "unknown");
    private static final String JAVA_VM_NAME = System.getProperty("java.vm.name", "unknown");

    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    private final String tracesEndpoint;
    private volatile long nextAllowedLogTime = 0;

    DDApi(final String host, final int port) {
        this.tracesEndpoint = "http://" + host + ":" + port + TRACES_ENDPOINT_V4;
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

        // TODO report lib verion?
        httpCon.setRequestProperty(DATADOG_META_TRACER_VERSION, "finagle-reporter");

        return httpCon;
    }

    /**
     * Send traces to the DD agent
     *
     * @param traces the traces to be sent
     * @return the staus code returned
     */
    void sendTraces(final List<List<Span>> traces) {
        log.debug("Sending traces {}", traces);
        final int totalSize = traces.size();
        try {
            final HttpURLConnection httpCon = getHttpURLConnection(tracesEndpoint);
            httpCon.setRequestProperty(X_DATADOG_TRACE_COUNT, String.valueOf(totalSize));

            try (final OutputStream out = httpCon.getOutputStream()) {
                objectMapper.writeValue(out, traces);
                out.flush();
            }

            String responseString = null;
            {
                final BufferedReader responseReader =
                        new BufferedReader(
                                new InputStreamReader(httpCon.getInputStream(), StandardCharsets.UTF_8));
                final StringBuilder sb = new StringBuilder();

                String line = null;
                while ((line = responseReader.readLine()) != null) {
                    sb.append(line);
                }
                skipAllContent(httpCon);
                responseReader.close();

                responseString = sb.toString();
            }

            final int responseCode = httpCon.getResponseCode();
            if (responseCode != 200) {
                if (log.isTraceEnabled()) {
                    log.trace(
                            "Error while sending {} of {} traces to the DD agent. Status: {}, ResponseMessage: ",
                            traces.size(),
                            totalSize,
                            responseCode,
                            httpCon.getResponseMessage());
                } else if (nextAllowedLogTime < System.currentTimeMillis()) {
                    nextAllowedLogTime = System.currentTimeMillis() + MILLISECONDS_BETWEEN_ERROR_LOG;
                    log.warn(
                            "Error while sending {} of {} traces to the DD agent. Status: {} (going silent for {} seconds)",
                            traces.size(),
                            totalSize,
                            responseCode,
                            httpCon.getResponseMessage(),
                            TimeUnit.MILLISECONDS.toMinutes(MILLISECONDS_BETWEEN_ERROR_LOG));
                }
                return;
            }

            log.trace("Succesfully sent {} of {} traces to the DD agent.", traces.size(), totalSize);

            try {
                if (!"".equals(responseString.trim())
                        && !"OK".equalsIgnoreCase(responseString.trim())) {
                    final JsonNode response = objectMapper.readTree(responseString);
                }
            } catch (final IOException e) {
                log.warn("Failed to parse DD agent response: " + responseString, e);
            }

        } catch (final IOException e) {
            if (log.isWarnEnabled()) {
                log.trace(
                        "Error while sending {} of {} traces to the DD agent.", traces.size(), totalSize, e);
            } else if (nextAllowedLogTime < System.currentTimeMillis()) {
                nextAllowedLogTime = System.currentTimeMillis() + MILLISECONDS_BETWEEN_ERROR_LOG;
                log.warn(
                        "Error while sending {} of {} traces to the DD agent. {}: {} (going silent for {} minutes)",
                        traces.size(),
                        totalSize,
                        e.getClass().getName(),
                        e.getMessage(),
                        TimeUnit.MILLISECONDS.toMinutes(MILLISECONDS_BETWEEN_ERROR_LOG));
            }
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

    @Override
    public String toString() {
        return "DDApi { tracesEndpoint=" + tracesEndpoint + " }";
    }

    public void sendTrace(PendingTrace pendingTrace) {
        // FIXME queue to bulk
        try {
            objectMapper.writeValue(System.out, pendingTrace.getSpans());
        } catch (IOException e) {
            // error sending spans
            e.printStackTrace();
        }

    }
}
