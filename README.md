# DataDog Finagle Reporter

[![Release](https://jitpack.io/v/michaelahlers/datadog-finagle.svg)](https://jitpack.io/#michaelahlers/datadog-finagle)

Reports Finagle traces to Datadog

## Installation

Add these settings:

```
resolvers += "JitPack".at("https://jitpack.io")
libraryDependencies += "com.github.michaelahlers" %% "datadog-finagle_19.12" % "{version}"
```

Finagle will autodetect the tracer.

## Options Supported

| | System Property | Environment Variable|
| --- | --- | --- |
| Service name | dd.service.name| DD_SERVICE_NAME|
| Agent host | dd.agent.host | DD_AGENT_HOST |
| Agent port | dd.agent.port | DD_AGENT_PORT |
| Trace Analytics | dd.trace.analytics.enabled | DD_TRACE_ANALYTICS_ENABLED |

## Logging

Logging is handled using the SLF4J api.  Set the log level of `datadog.trace.finagle` to `DEBUG` to see debug logs.

## About this Fork

The upstream project at [`DataDog/datadog-finagle`][github-datadog-datadog-finagle], built with Maven, [depends explicitly on Scala 2.12 binaries of Finagle](https://github.com/DataDog/datadog-finagle/blob/b083b7407ff4be13358817213f98574327a0f943/pom.xml#L57-L68) and elides that detail in its published artifacts (_i.e._, Scala's version suffix isn't preserved). In practice, this raises compatibility issues with projects wishing to use the library with Scala 2.13 or newer.

This fork was borne of the need for artifacts supporting multiple Scala minor versions (_e.g._, 2.12, 2.13) and newer versions of Finagle and Finatra. That's accomplished by revising this project to build with SBT, and—while doing so—introduce a few additional conveniences. Eventually, this fork will seek to support multiple major-minor versions of Finagle. 

[github-datadog-datadog-finagle]: https://github.com/DataDog/datadog-finagle
