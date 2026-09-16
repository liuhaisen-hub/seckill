package trace

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// 链路追踪设置
type Config struct {
	Enable      bool // 是否开启，没有开启就为nil,
	ServiceName string
	Endpoint    string  // 整个链路推送到哪里
	SampleRate  float64 // 采样率
}

// 全局实现一个Provider
type TracerProvider struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer // 本服务自己的命名
}

// 初始化
func NewTracerProvier(cfg *Config) (*TracerProvider, error) {
	if !cfg.Enable {
		// 未开启，返回nil。 oepnTelemetry自动带零值处理，不会奔溃，退化为no-op
		return &TracerProvider{}, nil
	}
	var exporter sdktrace.SpanExporter
	var err error
	switch cfg.Endpoint {
	case "":
		// 本地开发, 打印控制台
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "jaeger":
		// 本机的jaeger
		exporter, err = jaeger.New(jaeger.WithAgentEndpoint())
	default:
		exporter, err = otlptracegrpc.New(context.Background(),
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
			otlptracegrpc.WithInsecure(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}
	sampleRate := cfg.SampleRate
	if sampleRate <= 0 || sampleRate > 1 {
		// 采用率退化
		sampleRate = 1.0
	}
	res, err := resource.New(context.Background(), resource.WithAttributes(semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion("v1.0.0")))
	if err != nil {
		return nil, fmt.Errorf("faile to create resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))),
	)
	otel.SetTracerProvider(tp) // ④ 注册为进程级全局单例
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, // ⑤ W3C traceparent（跨服务主力）
		propagation.Baggage{},      // ⑥ W3C baggage（跨服务业务键值）
	))
	return &TracerProvider{
		provider: tp,
		tracer:   tp.Tracer(cfg.ServiceName),
	}, nil
}

// 获取tracer
func (tp *TracerProvider) Tracer() trace.Tracer {
	if tp.tracer == nil {
		return otel.Tracer("default")
	}
	return tp.tracer
}

// 停止收集
func (tp *TracerProvider) Shutdown(ctx context.Context) error {
	if tp.provider != nil {
		return tp.provider.Shutdown(ctx)
	}
	return nil
}

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("").Start(ctx, name, opts...)
}

func AddSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attrs...)
}

func RecordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
}

func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

func NewSpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{})
}

type TraceID string

func GetTraceID(ctx context.Context) TraceID {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().HasTraceID() {
		return TraceID(span.SpanContext().TraceID().String())
	}
	return ""
}
