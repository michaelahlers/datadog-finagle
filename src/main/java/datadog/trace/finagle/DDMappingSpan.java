package datadog.trace.finagle;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DDMappingSpan {
    private static final Logger log = LoggerFactory.getLogger(DDMappingSpan.class);

    private final Map<String, BigInteger> spanMapping;
    private final Span delegateSpan;

    DDMappingSpan(final Span delegateSpan, final Map<String, BigInteger> spanMapping) {
        this.delegateSpan = delegateSpan;
        this.spanMapping = spanMapping;
    }

    /**
     * Parses a 1 to 32 character lower-hex string with no prefix into an unsigned long, tossing any
     * bits higher than 64.
     */
    public static BigInteger lowerHexToUnsignedLong(final String lowerHex) {
        final int length = lowerHex.length();
        if (length < 1 || length > 32) {
            throw isntLowerHexLong(lowerHex);
        }

        // trim off any high bits
        final int beginIndex = length > 16 ? length - 16 : 0;

        return new BigInteger(lowerHex.substring(beginIndex), 16);
    }

    static NumberFormatException isntLowerHexLong(final String lowerHex) {
        throw new NumberFormatException(
                lowerHex + " should be a 1 to 32 character lower-hex string with no prefix");
    }

    @JsonGetter("start")
    public long getStartTime() {
        return TimeUnit.MICROSECONDS.toNanos(delegateSpan.timestampAsLong());
    }

    @JsonGetter("duration")
    public long getDurationNano() {
        final long duration = TimeUnit.MICROSECONDS.toNanos(delegateSpan.durationAsLong());

        return duration == 0 ? 1 : duration;
    }

    @JsonGetter("service")
    public String getServiceName() {
        final Map<String, String> tags = delegateSpan.tags();
        if (tags.containsKey("redis.args")) {
            return "redis";
        }

        if (tags.containsKey("sql.query")) {
            return "sql";
        }

        return delegateSpan.localServiceName();
    }

    /**
     * This method only returns the lower 64 bits of the trace id, so that is all that will be sent to
     * Datadog.
     *
     * @return
     */
    @JsonGetter("trace_id")
    public BigInteger getTraceId() {
        return lowerHexToUnsignedLong(delegateSpan.traceId());
    }

    /**
     * This method only returns the lower 64 bits of the span id, so that is all that will be sent to
     * Datadog.
     *
     * @return
     */
    @JsonGetter("span_id")
    public BigInteger getSpanId() {
        final BigInteger replacedId = spanMapping.get(delegateSpan.id());
        if (replacedId != null) {
            log.info("Found mapping {} - {}", delegateSpan, replacedId);
            return replacedId;
        }

        return lowerHexToUnsignedLong(delegateSpan.id());
    }

    @JsonGetter("parent_id")
    public BigInteger getParentId() {
        if (delegateSpan.parentId() == null) {
            return BigInteger.ZERO;
        }

        // If the span id was remapped, the span id is actually the parent
        final BigInteger replacedId = spanMapping.get(delegateSpan.id());
        if (replacedId != null) {
            return lowerHexToUnsignedLong(delegateSpan.id());
        } else {
            return lowerHexToUnsignedLong(delegateSpan.parentId());
        }
    }

    @JsonGetter("resource")
    public String getResourceName() {
        final Map<String, String> tags = delegateSpan.tags();
        if (tags.containsKey("sql.query")) {
            return tags.get("sql.query");
        }
        if (tags.containsKey("cassandra.query")) {
            return tags.get("cassandra.query");
        }
        if (tags.containsKey("db.statement")) {
            // Using Opentracing?
            return tags.get("db.statement");
        }
        if (tags.containsKey("redis.args")) {
            return delegateSpan.name() + " " + tags.get("redis.args");
        }
        if (tags.containsKey("http.uri") || tags.containsKey("http.path")) {
            String path = tags.get("http.uri") == null ? tags.get("http.path") : tags.get("http.uri");
            path = path == null ? "" : path.trim();

            String normalizedPath = normalizePath(rawPathFromUrlString(path));

            return delegateSpan.name().toUpperCase() + " " + normalizedPath;
        }

        if (tags.containsKey("channel")) {
            return tags.get("channel");
        }
        return delegateSpan.name();
    }

    @JsonGetter("name")
    public String getOperationName() {
        final Map<String, String> tags = delegateSpan.tags();
        if (tags.containsKey("sql.query")) {
            return "sql.query";
        }
        if (tags.containsKey("cassandra.query")) {
            return "cassandra.query";
        }
        if (tags.containsKey("db.statement")) {
            // Using Opentracing?
            return tags.get("db.statement");
        }
        if (tags.containsKey("redis.args")) {
            return "redis.query";
        }
        if (tags.containsKey("http.uri") || tags.containsKey("http.path")) {
            if (delegateSpan.kind().equals(Span.Kind.SERVER)) {
                return "server.request";
            } else if (delegateSpan.kind().equals(Span.Kind.CLIENT)) {
                return "client.request";
            }
        }
        if (tags.containsKey("channel")) {
            if (delegateSpan.kind().equals(Span.Kind.CONSUMER)) {
                return "channel.receive";
            } else if (delegateSpan.kind().equals(Span.Kind.PRODUCER)) {
                return "channel.send";
            }
        }
        // DataDog does not support trace names without alphanumerical characters
        return delegateSpan.name();
    }

    @JsonGetter("sampling_priority")
    @JsonInclude(Include.NON_NULL)
    public Integer getSamplingPriority() {
        return Boolean.TRUE.equals(delegateSpan.debug()) ? 2 : 1;
    }

    @JsonGetter
    public Map<String, String> getMeta() {
        Map<String, String> tagMap = new HashMap<>(delegateSpan.tags());

        Endpoint remoteEndpoint = delegateSpan.remoteEndpoint();
        if (remoteEndpoint != null) {
            if (remoteEndpoint.port() != null) {
                tagMap.put("peer.port", remoteEndpoint.port().toString());
            }

            if (remoteEndpoint.ipv4() != null) {
                tagMap.put("peer.ipv4", remoteEndpoint.ipv4());
            }

            if (remoteEndpoint.ipv6() != null) {
                tagMap.put("peer.ipv6", remoteEndpoint.ipv6());
            }
        }

        if (tagMap.containsKey("http.uri")) {
            tagMap.put("http.method", delegateSpan.name().toUpperCase());
        }


        return tagMap;
    }

    @JsonGetter
    public String getType() {
        if (delegateSpan.kind() == null) {
            return "other";
        }
        switch (delegateSpan.kind()) {
            case CONSUMER:
            case PRODUCER:
                return "queue";
            case CLIENT:
                if (delegateSpan.tags().containsKey("sql.query")) {
                    return "sql";
                }
                if (delegateSpan.tags().containsKey("cassandra.query")) {
                    // brave-cassandra
                    return "cassandra";
                }
                if (delegateSpan.tags().containsKey("http.path")
                        || delegateSpan.tags().containsKey("http.uri")) {
                    return "http";
                }
                if (delegateSpan.tags().containsKey("redis.args")) {
                    return "redis";
                }
                break;
            case SERVER:
                if (delegateSpan.tags().containsKey("http.path")
                        || delegateSpan.tags().containsKey("http.uri")) {
                    return "web";
                }
        }
        return null;
    }

    @JsonGetter
    public int getError() {
        return delegateSpan.tags().containsKey("error") ? 1 : 0;
    }

    // Copied from datadog.opentracing.decorators.URLAsResourceName
    // Matches any path segments with numbers in them. (exception for versioning: "/v1/")
    public static final Pattern PATH_MIXED_ALPHANUMERICS =
            Pattern.compile("(?<=/)(?![vV]\\d{1,2}/)(?:[^\\/\\d\\?]*[\\d]+[^\\/\\?]*)");

    private static String rawPathFromUrlString(final String url) {
        // Get the path without host:port
        // url may already be just the path.

        if (url.isEmpty()) {
            return "/";
        }

        final int queryLoc = url.indexOf("?");
        final int fragmentLoc = url.indexOf("#");
        final int endLoc;
        if (queryLoc < 0) {
            if (fragmentLoc < 0) {
                endLoc = url.length();
            } else {
                endLoc = fragmentLoc;
            }
        } else {
            if (fragmentLoc < 0) {
                endLoc = queryLoc;
            } else {
                endLoc = Math.min(queryLoc, fragmentLoc);
            }
        }

        final int protoLoc = url.indexOf("://");
        if (protoLoc < 0) {
            return url.substring(0, endLoc);
        }

        final int pathLoc = url.indexOf("/", protoLoc + 3);
        if (pathLoc < 0) {
            return "/";
        }

        if (queryLoc < 0) {
            return url.substring(pathLoc);
        } else {
            return url.substring(pathLoc, endLoc);
        }
    }

    // Method to normalise the url string
    private static String normalizePath(final String path) {
        if (path.isEmpty() || path.equals("/")) {
            return "/";
        }

        return PATH_MIXED_ALPHANUMERICS.matcher(path).replaceAll("?");
    }

    @Override
    public String toString() {
        return "DDMappingSpan={"
                + "startTime="
                + getStartTime()
                + ", durationNano="
                + getDurationNano()
                + ", serviceName="
                + getServiceName()
                + ", traceId="
                + getTraceId()
                + ", spanId="
                + getSpanId()
                + ", parentId="
                + getParentId()
                + ", resourceName='"
                + getResourceName()
                + "\'"
                + ", operationName='"
                + getOperationName()
                + "\'"
                + ", samplingPriority="
                + getSamplingPriority()
                + ", meta="
                + getMeta()
                + ", type="
                + getType()
                + ", delegateSpan="
                + delegateSpan.toString();
    }
}
