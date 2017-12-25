// Package opentracing provides wrappers for OpenTracing
package opentracing

import (
	"fmt"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/server"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"github.com/opentracing/opentracing-go/ext"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"

	"github.com/zipkin-go-opentracing"
)

type otWrapper struct {
	ot           opentracing.Tracer
	traceRequest traceRequest
	client.Client
}
type ClientTrace struct {
	Tracer    opentracing.Tracer
	Collector zipkintracer.Collector
	Span      opentracing.Span
	Ctx       context.Context
}

type traceRequest func(ctx context.Context) context.Context

func GetTrace(serviceName, hostStr string) (opentracing.Tracer, zipkintracer.Collector) {
	zipkinhost := fmt.Sprintf("http://%s/api/v1/spans", hostStr)
	collector, err := zipkin.NewHTTPCollector(zipkinhost)

	if err != nil {
		fmt.Printf("unable to create Zipkin HTTP collector: %+v\n", err)
	}
	recorder := zipkin.NewRecorder(collector, true, "local", serviceName)
	// Create our tracer.
	tracer, err := zipkin.NewTracer(
		recorder,
	)
	return tracer, collector
}
func GetClientTrace(serviceName, hostStr string, ctx context.Context) (*ClientTrace) {
	traceObj := &ClientTrace{}
	traceObj.Tracer, traceObj.Collector = GetTrace(serviceName, hostStr)
	opentracing.InitGlobalTracer(traceObj.Tracer)
	traceObj.Span = opentracing.StartSpan("run")
	// Put root span in context so it will be used in our calls to the client.
	traceObj.Ctx = opentracing.ContextWithSpan(ctx, traceObj.Span)
	return traceObj
}

func CloseTrace(span opentracing.Span, collector zipkintracer.Collector) {
	// Finish our CLI span
	span.Finish()
	collector.Close()
}

func traceToCtx(ot opentracing.Tracer) traceRequest {
	return func(ctx context.Context) context.Context {
		if span := opentracing.SpanFromContext(ctx); span != nil {
			ext.SpanKindRPCClient.Set(span)
			md := metadata.Metadata{}
			if err := span.Tracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(md)); err != nil {
				return ctx
			}
			ctx = metadata.NewContext(ctx, md)

		} else {
			fmt.Println("err", span)
		}
		return ctx
	}
}

func traceIntoContext(ctx context.Context, tracer opentracing.Tracer, name string) (context.Context, error) {
	md, _ := metadata.FromContext(ctx)
	var sp opentracing.Span
	wireContext, err := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(md))
	if err != nil {
		sp = tracer.StartSpan(name)
	} else {
		sp = tracer.StartSpan(name, ext.RPCServerOption(wireContext))
	}
	defer sp.Finish()
	if err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, opentracing.TextMapCarrier(md)); err != nil {
		return nil, err
	}
	ctx = metadata.NewContext(ctx, md)

	return ctx, nil
}

func (o *otWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	name := req.Method()
	span, ctx := opentracing.StartSpanFromContext(ctx, name)
	defer span.Finish()
	ctx = o.traceRequest(ctx)
	return o.Client.Call(ctx, req, rsp, opts...)
}

func (o *otWrapper) Publish(ctx context.Context, p client.Publication, opts ...client.PublishOption) error {
	name := fmt.Sprintf("Pub to %s", p.Topic())
	ctx, err := traceIntoContext(ctx, o.ot, name)
	if err != nil {
		return err
	}
	return o.Client.Publish(ctx, p, opts...)
}

func NewClientWrapper(tracer opentracing.Tracer) client.Wrapper {
	return func(c client.Client) client.Client {
		return &otWrapper{tracer, traceToCtx(tracer), c}
	}
}

// NewHandlerWrapper accepts an opentracing Tracer and returns a Handler Wrapper
func NewHandlerWrapper(ot opentracing.Tracer) server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) error {
			name := fmt.Sprintf("%s.%s", req.Service(), req.Method())
			ctx, err := traceIntoContext(ctx, ot, name)
			if err != nil {
				return err
			}
			return h(ctx, req, rsp)
		}
	}
}

// NewSubscriberWrapper accepts an opentracing Tracer and returns a Subscriber Wrapper
func NewSubscriberWrapper(ot opentracing.Tracer) server.SubscriberWrapper {
	return func(next server.SubscriberFunc) server.SubscriberFunc {
		return func(ctx context.Context, msg server.Publication) error {
			name := "Pub to " + msg.Topic()
			ctx, err := traceIntoContext(ctx, ot, name)
			if err != nil {
				return err
			}
			return next(ctx, msg)
		}
	}
}
