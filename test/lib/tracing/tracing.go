package tracing

import (
	"os"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	tracingconfig "knative.dev/pkg/tracing/config"

	"knative.dev/eventing/pkg/tracing"
)

func GenerateEnv() corev1.EnvVar {
	return corev1.EnvVar{
		Name:  "ZIPKIN_E2E_ENDPOINT",
		Value: ZipkinTestEndpoint(),
	}
}

func ZipkinTestEndpoint() string {
	endpoint := os.Getenv("ZIPKIN_E2E_ENDPOINT")
	if endpoint == "" {
		return "http://zipkin.istio-system.svc.cluster.local:9411/api/v2/spans"
	}
	return endpoint
}

func TracingConfig() *tracingconfig.Config {
	return &tracingconfig.Config{
		Backend:        tracingconfig.Zipkin,
		Debug:          true,
		SampleRate:     1.0,
		ZipkinEndpoint: ZipkinTestEndpoint(),
	}
}

func SetupTestImageTracing(logger *zap.SugaredLogger) error {
	return tracing.SetupStaticPublishing(logger, "", TracingConfig())
}
