package datadog.trace.finagle;

import com.google.auto.service.AutoService;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.Stack;
import com.twitter.finagle.StackTransformer;
import com.twitter.finagle.tracing.TraceInitializerFilter;

@AutoService(StackTransformer.class)
public class FilterInstaller extends StackTransformer {
    @Override
    public String name() {
        return "Trace Filter Installer";
    }

    @Override
    public <Req, Rep> Stack<ServiceFactory<Req, Rep>> apply(Stack<ServiceFactory<Req, Rep>> stack) {
        return stack.replace(TraceInitializerFilter.role(), new ServerTraceInitializer());
    }
}
