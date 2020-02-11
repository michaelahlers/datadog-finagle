package datadog.trace.finagle;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.twitter.finagle.service.TimeoutFilter;
import com.twitter.finagle.tracing.Annotation;
import com.twitter.finagle.tracing.Record;
import datadog.trace.api.Config;
import datadog.trace.api.DDSpanTypes;
import datadog.trace.api.DDTags;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Span {
  private static final Logger log = LoggerFactory.getLogger(Span.class);

  // Copied from datadog.opentracing.decorators.URLAsResourceName
  // Matches any path segments with numbers in them. (exception for versioning: "/v1/")
  private static final Pattern PATH_MIXED_ALPHANUMERICS =
      Pattern.compile("(?<=/)(?![vV]\\d{1,2}/)(?:[^\\/\\d\\?]*[\\d]+[^\\/\\?]*)");

  public enum Kind {
    SERVER,
    CLIENT,
    PRODUCER,
    CONSUMER;
  }

  private final Map<String, String> tags = new HashMap<>();

  private final PendingTrace trace;
  private final BigInteger traceId;
  private final BigInteger parentId;
  private final BigInteger spanId;
  private String type;
  private Kind kind;
  private String assignedServiceName;
  private long startTime;
  private long endTime;
  private String name;
  private InetSocketAddress localAddress;
  private InetSocketAddress clientAddress;
  private InetSocketAddress serverAddress;
  private InetSocketAddress messagingAddress;

  public Span(PendingTrace trace, BigInteger traceId, BigInteger parentId, BigInteger spanId) {
    this.traceId = traceId;
    this.parentId = parentId;
    this.spanId = spanId;

    this.trace = trace;
  }

  public void addRecord(Record record) {
    if (isComplete()) {
      log.warn("Record added to completed span: {}", record);
    }

    if (startTime == 0) {
      startTime = record.timestamp().inNanoseconds();
    }

    Annotation annotation = record.annotation();
    if (Annotation.ClientSend$.MODULE$.equals(annotation)
        || Annotation.ClientRecv$.MODULE$.equals(annotation)) {
      kind = Kind.CLIENT;
    } else if (Annotation.ServerRecv$.MODULE$.equals(annotation)
        || Annotation.ServerSend$.MODULE$.equals(annotation)) {
      kind = Kind.SERVER;
      tags.put(Config.LANGUAGE_TAG_KEY, Config.LANGUAGE_TAG_VALUE);
    } else if (annotation instanceof Annotation.Rpc) {
      name = ((Annotation.Rpc) annotation).name();
      type = DDSpanTypes.RPC;
    } else if (annotation instanceof Annotation.ServiceName) {
      assignedServiceName = ((Annotation.ServiceName) annotation).service();
    } else if (annotation instanceof Annotation.WireRecvError) {
      String error = ((Annotation.WireRecvError) annotation).error();
      tags.put(DDTags.ERROR_MSG, "Wire Receive Error: " + error);
    } else if (annotation instanceof Annotation.ClientRecvError) {
      String error = ((Annotation.ClientRecvError) annotation).error();
      tags.put(DDTags.ERROR_MSG, "Client Receive Error: " + error);
    } else if (annotation instanceof Annotation.ServerSendError) {
      String error = ((Annotation.ServerSendError) annotation).error();
      tags.put(DDTags.ERROR_MSG, "Server Send  Error: " + error);
    } else if (annotation instanceof Annotation.LocalAddr) {
      localAddress = ((Annotation.LocalAddr) annotation).ia();
    } else if (annotation instanceof Annotation.ClientAddr) {
      clientAddress = ((Annotation.ClientAddr) annotation).ia();
    } else if (annotation instanceof Annotation.ServerAddr) {
      serverAddress = ((Annotation.ServerAddr) annotation).ia();
    } else if (annotation instanceof Annotation.BinaryAnnotation) {
      Object value = ((Annotation.BinaryAnnotation) annotation).value();
      if (value instanceof Number || value instanceof Boolean || value instanceof String) {
        tags.put(
            convertTagName(((Annotation.BinaryAnnotation) annotation).key()), value.toString());
      }
    }

    // Finishing spans
    // srv/response_payload_bytes comes after ServerSend so use that instead
    if (Annotation.ClientRecv$.MODULE$.equals(annotation)
        || (annotation instanceof Annotation.BinaryAnnotation
            && "srv/response_payload_bytes"
                .equals(((Annotation.BinaryAnnotation) annotation).key()))
        || (annotation instanceof Annotation.Message
            && TimeoutFilter.TimeoutAnnotation()
                .equals(((Annotation.Message) annotation).content()))) {

      endTime = record.timestamp().inNanoseconds();
    }

    // TODO MS and MR Kind.Producer, Kind.Consumer
    /*
        if (Annotation.WireSend$.MODULE$.equals(annotation)) {
          span.addAnnotation(record.timestamp(), "ws");
        } else if (Annotation.WireRecv$.MODULE$.equals(annotation)) {
          span.addAnnotation(record.timestamp(), "wr");
        }  else if (Annotation.ClientSendFragment$.MODULE$.equals(annotation)) {
          span.addAnnotation(record.timestamp(), "csf");
        } else if (Annotation.ClientRecvFragment$.MODULE$.equals(annotation)) {
          span.addAnnotation(record.timestamp(), "crf");
        } else if (Annotation.ServerSendFragment$.MODULE$.equals(annotation)) {
          span.addAnnotation(record.timestamp(), "ssf");
        } else if (Annotation.ServerRecvFragment$.MODULE$.equals(annotation)) {
          span.addAnnotation(record.timestamp(), "srf");
        } else if (annotation instanceof Annotation.Message) {
          String value = ((Annotation.Message) annotation).content();
          span.addAnnotation(record.timestamp(), value);
        }
    */
  }

  private static String convertTagName(String original) {
    String converted = original.replaceAll("/", ".");
    if ("http.uri".equals(converted)) {
      converted = "http.url";
    }

    converted = converted.replace("clnt.finagle", "client");
    converted = converted.replace("srv.finagle", "server");
    converted = converted.replace("clnt.", "client.");
    converted = converted.replace("srv.", "server.");

    return converted;
  }

  @JsonIgnore
  public boolean isComplete() {
    return endTime != 0;
  }

  @JsonGetter("start")
  public long getStartTime() {
    return startTime;
  }

  @JsonGetter("duration")
  public long getDurationNano() {
    return Math.max(endTime - startTime, 1);
  }

  @JsonGetter("service")
  public String getServiceName() {
    if (tags.containsKey("redis.args")) {
      return "redis";
    }

    if (tags.containsKey("sql.query")) {
      return "sql";
    }

    return trace.getServiceName();
  }

  @JsonGetter("trace_id")
  public BigInteger getTraceId() {
    return traceId;
  }

  @JsonGetter("span_id")
  public BigInteger getSpanId() {
    final BigInteger replacedId = trace.getRemappedSpanIds().get(spanId);
    if (replacedId != null) {
      return replacedId;
    }

    return spanId;
  }

  @JsonGetter("parent_id")
  public BigInteger getParentId() {
    // Finagle sets span id equal to the parent when there is no parent
    if (spanId.equals(parentId)) {
      return BigInteger.ZERO;
    }

    // If the current span was remapped, the original span id is the parent
    BigInteger replacedId = trace.getRemappedSpanIds().get(spanId);
    if (replacedId != null) {
      return spanId;
    }

    // If the parent span was remapped, use that instead
    replacedId = trace.getRemappedSpanIds().get(parentId);
    if (replacedId != null) {
      return replacedId;
    }

    // Default case: no remapping
    return parentId;
  }

  @JsonGetter("resource")
  public String getResourceName() {
    if (tags.containsKey("sql.query")) {
      return tags.get("sql.query");
    }

    if (tags.containsKey("cassandra.query")) {
      return tags.get("cassandra.query");
    }

    if (tags.containsKey("db.statement")) {
      return tags.get("db.statement");
    }

    if (tags.containsKey("redis.args")) {
      return "redis.query " + tags.get("redis.args");
    }

    if (tags.containsKey("http.url")) {
      String path = tags.get("http.url");
      String normalizedPath = normalizePath(rawPathFromUrlString(path.trim()));

      return name.toUpperCase() + " " + normalizedPath;
    }

    if (tags.containsKey("channel")) {
      return tags.get("channel");
    }
    return name;
  }

  @JsonGetter("name")
  public String getOperationName() {
    if (tags.containsKey("sql.query")) {
      return "sql.query";
    }
    if (tags.containsKey("cassandra.query")) {
      return "cassandra.query";
    }
    if (tags.containsKey("db.statement")) {
      return tags.get("db.statement");
    }
    if (tags.containsKey("redis.args")) {
      return "redis.query";
    }
    if (tags.containsKey("http.url")) {
      if (Kind.SERVER == kind) {
        return "finagle.server.request";
      } else if (Kind.CLIENT == kind) {
        return "finagle.client.request";
      }
    }
    if (tags.containsKey("channel")) {
      if (Kind.CONSUMER == kind) {
        return "channel.receive";
      } else if (Kind.PRODUCER == kind) {
        return "channel.send";
      }
    }

    return name;
  }

  @JsonGetter("sampling_priority")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getSamplingPriority() {
    return trace.getSamplingPriority();
  }

  @JsonGetter
  public Map<String, String> getMeta() {
    Map<String, String> tagMap = new HashMap<>(tags);

    addNetworkTags(tagMap);
    tagMap.put("component", "finagle-tracer");

    if (DDSpanTypes.HTTP_CLIENT.equals(getType()) || DDSpanTypes.HTTP_SERVER.equals(getType())) {
      tagMap.put("http.method", name);
    }

    if (kind == Kind.CLIENT && assignedServiceName != null) {
      tagMap.put("peer.service", assignedServiceName);
    }

    return tagMap;
  }

  private void addNetworkTags(Map<String, String> tagMap) {
    InetSocketAddress peerAddress = null;
    if (kind == Kind.SERVER) {
      peerAddress = clientAddress;
    } else if (kind == Kind.CLIENT) {
      peerAddress = serverAddress;
    }

    if (peerAddress != null) {
      if (peerAddress.getAddress() instanceof Inet6Address) {
        tagMap.put("peer.ipv6", peerAddress.getAddress().getHostAddress());
      } else {
        tagMap.put("peer.ipv4", peerAddress.getAddress().getHostAddress());
      }

      if (kind == Kind.CLIENT) {
        tagMap.put("peer.port", String.valueOf(peerAddress.getPort()));
      }
      tagMap.put("peer.hostname", peerAddress.getHostName());
    }
  }

  @JsonGetter
  public String getType() {
    if (kind == null) {
      return "other";
    }

    switch (kind) {
      case CONSUMER:
        return DDSpanTypes.MESSAGE_CONSUMER;
      case PRODUCER:
        return DDSpanTypes.MESSAGE_PRODUCER;
      case CLIENT:
        if (tags.containsKey("sql.query")) {
          return DDSpanTypes.SQL;
        }
        if (tags.containsKey("cassandra.query")) {
          return DDSpanTypes.CASSANDRA;
        }
        if (tags.containsKey("http.url")) {
          return DDSpanTypes.HTTP_CLIENT;
        }
        if (tags.containsKey("redis.args")) {
          return DDSpanTypes.REDIS;
        }
        break;
      case SERVER:
        if (tags.containsKey("http.url")) {
          return DDSpanTypes.HTTP_SERVER;
        }
    }

    if (type != null) {
      return type;
    }

    return "other";
  }

  @JsonGetter
  public int getError() {
    return tags.containsKey(DDTags.ERROR_MSG) ? 1 : 0;
  }

  @JsonIgnore
  public Kind getKind() {
    return kind;
  }

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
}
