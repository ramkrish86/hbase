/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.trace;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.htrace.core.SpanReceiver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.Configuration.SenderConfiguration;
import io.jaegertracing.internal.senders.SenderResolver;
import io.opentelemetry.exporters.zipkin.ZipkinSpanExporter;
import io.opentelemetry.opentracingshim.TraceShim;
import io.opentelemetry.sdk.correlationcontext.CorrelationContextManagerSdk;
import io.opentelemetry.sdk.trace.TracerSdkProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.trace.TracerProvider;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;

/**
 * This wrapper class provides functions for accessing htrace 4+ functionality in a simplified way.
 */
@InterfaceAudience.Private
public final class TraceUtil {
  private static io.jaegertracing.Configuration conf;
  public static final Logger LOG = LoggerFactory.getLogger(TraceUtil.class);
  private static Tracer tracer;
  private static TracerSdkProvider tracerProvider;
  public static final String HBASE_OPENTRACING_TRACER = "hbase.opentracing.tracer";
  public static final String HBASE_OPENTRACING_TRACER_DEFAULT = "jaeger";
  public static final String HBASE_OPENTRACING_MOCKTRACER = "mock";
  public static volatile boolean isclosed = false;
  private static Tracer foul_tracer=TraceShim.createTracerShim();
  private TraceUtil() {
  }
  public static void close()
  {
    isclosed=true;
  }
  public static void start() {
    isclosed=false;
  }
  public static void initTracer(Configuration c, String serviceName) {
    /*if (c != null) {
      conf = new HBaseHTraceConfiguration(c);
    }

    if (tracer == null && conf != null) {
      tracer = new Tracer.Builder("Tracer").conf(conf).build();
    }*/

    if(serviceName=="RegionServer") {
      conf = io.jaegertracing.Configuration.fromEnv(serviceName);
      if (!GlobalTracer.isRegistered()) {
        switch (c.get(HBASE_OPENTRACING_TRACER, HBASE_OPENTRACING_TRACER_DEFAULT)) {
          case HBASE_OPENTRACING_TRACER_DEFAULT:

            tracerProvider = TracerSdkProvider.builder().build();
            ZipkinSpanExporter exporter =
              ZipkinSpanExporter.newBuilder().setEndpoint("http://localhost:9411/api/v2/spans").setServiceName(serviceName).build();

            // JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.newBuilder()
            // .setServiceName(serviceName).setDeadlineMs(30000).build();
            tracerProvider.addSpanProcessor(SimpleSpanProcessor.newBuilder(exporter).build());

            tracer = TraceShim.createTracerShim(tracerProvider, new CorrelationContextManagerSdk());
            if(isclosed)
              tracer=foul_tracer;
            break;
          case HBASE_OPENTRACING_MOCKTRACER:
            tracer = new MockTracer();
            if(isclosed)
              tracer.close();
            break;
          default:
            throw new RuntimeException("Unexpected tracer");
        }
        LOG.debug("The tracer is " + tracer + " " + serviceName);
      }
    }
  }

  @VisibleForTesting
  public static void registerTracerForTest(Tracer tracer) {
    //TraceUtil.tracer = tracer;
    GlobalTracer.register(tracer);
  }

  public static Tracer getTracer() {
    if(isclosed)
      return foul_tracer;
    return tracer;
  }
  
  /*
   * static class NoopTracer implements Tracer {
   * @Override public ScopeManager scopeManager() { return NoopScopeManager.INSTANCE; }
   * @Override public Span activeSpan() { return null; }
   * @Override public SpanBuilder buildSpan(String operationName) { return NoopSpanBuilder.INSTANCE;
   * }
   * @Override public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {}
   * @Override public <C> SpanContext extract(Format<C> format, C carrier) { return
   * NoopSpanBuilder.INSTANCE; }
   * @Override public String toString() { return NoopTracer.class.getSimpleName(); } }
   */
 
  public static void convert() {
    LOG.info("Updating the tracer");
    //tracer = new NoopTracer();
  }

  
  /**
   * Wrapper method to create new Scope with the given description
   * @return Scope or null when not tracing
   */
  public static Scope createRootTrace(String description) {
    Span span  = (getTracer() == null) ? null : getTracer().buildSpan(description).start();
    if(span != null) {
      return getTracer().scopeManager().activate(span);
    }
    return null;
  }

  /**
   * Wrapper method to create new Scope with the given description
   * @return Scope or null when not tracing
   */
  
  public static Pair<Scope,Span> createTrace(String description) {
    if (getTracer().activeSpan() == null) {
      //LOG.warn("no existing span. Please trace the code and find out where to initialize the span " +description);
    }
    Span span  = (getTracer() == null) ? null : getTracer().buildSpan(description).start();
    Pair<Scope,Span> tracePair = new Pair (getTracer().scopeManager().activate(span),span);
    if(span != null) {
      return tracePair;
    }
    return null;
  }

  /**
   * Wrapper method to create new child Scope with the given description
   * and parent scope's spanId
   * @param span parent span
   * @return Scope or null when not tracing
   */
  public static Scope createTrace(String description, Span span) {
    if (span == null) {
      return createTrace(description).getFirst();
    }
    span =  (getTracer() == null) ? null
        : getTracer().buildSpan(description).asChildOf(span).start();
    if(span != null) {
      return getTracer().scopeManager().activate(span);
    }
    return null;
  }

  public static Scope createTrace(String description, SpanContext spanContext) {
    if(spanContext == null) return createTrace(description).getFirst();

    Span span  = (getTracer() == null) ? null : getTracer().buildSpan(description).
        asChildOf(spanContext).start();
    if(span != null) {
      return getTracer().scopeManager().activate(span);
    }
    return null;
  }

  public static void main(String[] args) {
    // SenderResolver.resolve();
    TraceUtil.initTracer(new Configuration(), "test");
    Pair<Scope,Span> tracePair=null;
    try {
      tracePair = createTrace("test");
      addTimelineAnnotation("testmsg");
    } finally {
      tracePair.getFirst().close();
      tracePair.getSecond().finish();
      //scope.().finish();
    }
  }

  /**
   * Wrapper method to add new sampler to the default tracer
   * @return true if added, false if it was already added
   */
  public static boolean addSampler(SamplerConfiguration sampler) {
    if (sampler == null) {
      return false;
    }
    io.jaegertracing.Configuration conf = io.jaegertracing.Configuration.fromEnv("PE");

    ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
    SenderConfiguration c = new SenderConfiguration();
    SenderResolver.resolve();
    io.jaegertracing.Configuration config =
        new io.jaegertracing.Configuration("PE").withSampler(sampler).withReporter(reporterConfig);
    conf = config.withSampler(sampler);
    io.opentracing.Tracer tracer = conf.getTracerBuilder().build();
    LOG.debug("Am i registered "+GlobalTracer.isRegistered());
    LOG.debug("Registering "+tracer.toString());
    GlobalTracer.register(tracer);
    return true;
  }

  /**
   * Wrapper method to add key-value pair to TraceInfo of actual span
   */
  public static void addKVAnnotation(String key, String value){
    Span span = getTracer().activeSpan();
    if (span != null) {
      span.setTag(key, value);
    }
  }

  /**
   * Wrapper method to add receiver to actual tracerpool
   * @return true if successfull, false if it was already added
   */
  public static boolean addReceiver(SpanReceiver rcvr) {
    //return (tracer == null) ? false : tracer.getTracerPool().addReceiver(rcvr);
    return false;
  }

  /**
   * Wrapper method to remove receiver from actual tracerpool
   * @return true if removed, false if doesn't exist
   */
  public static boolean removeReceiver(SpanReceiver rcvr) {
    //return (tracer == null) ? false : tracer.getTracerPool().removeReceiver(rcvr);
    return false;
  }

  /**
   * Wrapper method to add timeline annotiation to current span with given message
   */
  public static void addTimelineAnnotation(String msg) {
    Span span = getTracer().activeSpan();
    if (span != null) {
      span.log(msg);
    }
  }

  /**
   * Wrap runnable with current tracer and description
   * @param runnable to wrap
   * @return wrapped runnable or original runnable when not tracing
   */
  public static Runnable wrap(Runnable runnable, String description) {
    //return (tracer == null) ? runnable : tracer.wrap(runnable, description);
    LOG.debug("The provided serialized context was null or empty");
    return null;
  }

  public static SpanContext byteArrayToSpanContext(byte[] byteArray) {
    if (byteArray == null || byteArray.length == 0) {
      LOG.debug("The provided serialized context was null or empty");
      return null;
    }

    SpanContext context = null;
    ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);

    try {
      ObjectInputStream objStream = new ObjectInputStream(stream);
      TextMapExtractAdapter carrier = new TextMapExtractAdapter((Map<String, String>) objStream.readObject());

      context = tracer.extract(Format.Builtin.TEXT_MAP_EXTRACT, carrier);
      carrier.iterator();
    } catch (Exception e) {
      LOG.warn("Could not deserialize context {}", e);
    }
    return context;
  }

  public static byte[] spanContextToByteArray(SpanContext context) {
    if (context == null) {
      LOG.debug("No SpanContext was provided");
      return null;
    }

    Map<String, String> carrier = new HashMap<String, String>();
    tracer.inject(context, Format.Builtin.TEXT_MAP_INJECT, new TextMapInjectAdapter(carrier));
    if (carrier.isEmpty()) {
      LOG.warn("SpanContext was not properly injected by the Tracer.");
      return null;
    }

    byte[] byteArray = null;
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try {
      ObjectOutputStream objStream = new ObjectOutputStream(stream);
      objStream.writeObject(carrier);
      objStream.flush();

      byteArray = stream.toByteArray();
      LOG.debug("SpanContext serialized, resulting byte length is {}", byteArray.length);
    } catch (IOException e) {
      LOG.warn("Could not serialize context {}", e);
    }

    return byteArray;
  }

  static class JaegerTracerProvider implements TracerProvider {
    private  io.jaegertracing.Configuration conf;
    private JaegerTracerProvider() {
      
    }
    private static JaegerTracerProvider INSTANCE = new JaegerTracerProvider();
    @Override
    public io.opentelemetry.trace.Tracer get(String instrumentationName) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public io.opentelemetry.trace.Tracer get(String instrumentationName,
        String instrumentationVersion) {
      // TODO Auto-generated method stub
      return null;
    }
    
    public void setConf(io.jaegertracing.Configuration conf) {
      this.conf = conf;
    }
    
  }
}
