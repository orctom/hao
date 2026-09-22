import logging

from . import config, versions

LOGGER = logging.getLogger(__name__)

_tracer = None


def setup_tracing():
    """Initialize OpenTelemetry tracing. Must be called before any instrumented code."""

    try:
        from opentelemetry import trace
    except ModuleNotFoundError:
        LOGGER.info("[otel] opentelemetry not installed")
        return

    conf = config.get('otel', {})
    enabled, endpoint, name = conf.get('enabled'), conf.get('endpoint'), conf.get('name')
    LOGGER.info(f"[otel] enabled={enabled}, endpoint={endpoint}")
    if enabled and endpoint:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

        sample_rate = conf.get('otel.sample_rate', 1)

        resource = Resource.create({
            "service.name": name,
            "service.version": versions.get_version(),
            "deployment.environment": config.ENV or 'dev',
        })

        sampler = ParentBasedTraceIdRatio(rate=sample_rate)
        provider = TracerProvider(resource=resource, sampler=sampler)

        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))

        trace.set_tracer_provider(provider)
        LOGGER.info(f"[otel] tracing initialized, sample_rate={sample_rate}")

    global _tracer
    _tracer = trace.get_tracer(name, versions.get_version())


def get_tracer():
    global _tracer
    if not _tracer:
        setup_tracing()
    return _tracer
