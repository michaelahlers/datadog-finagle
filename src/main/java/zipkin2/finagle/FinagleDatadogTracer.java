package zipkin2.finagle;

import com.google.auto.service.AutoService;
import com.twitter.finagle.stats.DefaultStatsReceiver$;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.tracing.Tracer;
import datadog.trace.finagle.DatadogReporter;

/*
The constructor we want to use is package private so this class needs to be in
zipkin2.finagle
 */
@AutoService(Tracer.class)
public class FinagleDatadogTracer extends ZipkinTracer {
    public FinagleDatadogTracer() {
        this(new DatadogBasedConfig(), DefaultStatsReceiver$.MODULE$.get().scope("zipkin.datadog"));
    }

    public FinagleDatadogTracer(
            final DatadogBasedConfig datadogBasedConfig, final StatsReceiver stats) {
        super(new DatadogReporter(datadogBasedConfig.getHost(), datadogBasedConfig.getPort()),
                datadogBasedConfig,
                stats);
    }

    public static class DatadogBasedConfig implements Config {
        @Override
        public String localServiceName() {
            return datadog.trace.api.Config.get().getServiceName();
        }

        @Override
        public float initialSampleRate() {
            final Double configSampleRate = datadog.trace.api.Config.get().getTraceSamplingDefaultRate();
            return configSampleRate == null ? 1f : configSampleRate.floatValue();
        }

        public String getHost() {
            return datadog.trace.api.Config.get().getAgentHost();
        }

        public int getPort() {
            return datadog.trace.api.Config.get().getAgentPort();
        }
    }
}

