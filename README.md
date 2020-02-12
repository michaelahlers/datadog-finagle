# DataDog Finagle Reporter

Reports Finagle traces to Datadog

Installation:
---

Add 
```
"com.datadoghq" % "dd-finagle" % "{latest version}"
```

to your `build.sbt`.  Finagle will autodetect the tracer.

Options Supported:
---

| | System Property | Environment Variable|
| --- | --- | --- |
| Service name | dd.service.name| DD_SERVICE_NAME|
| Agent host | dd.agent.host | DD_AGENT_HOST |
| Agent port | dd.agent.port | DD_AGENT_PORT |
| Trace Analytics | dd.trace.analytics.enabled | DD_TRACE_ANALYTICS_ENABLED |

Logging:
---

Logging is handled using the SLF4J api.  Set the log level of `datadog.trace.finagle` to `DEBUG` to see debug logs.
