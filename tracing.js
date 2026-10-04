const { NodeSDK } = require("@opentelemetry/sdk-node");
const {
    getNodeAutoInstrumentations
} = require("@opentelemetry/auto-instrumentations-node");
const {
    OTLPTraceExporter
} = require("@opentelemetry/exporter-trace-otlp-proto");

const {
    OTLPMetricExporter
} = require("@opentelemetry/exporter-metrics-otlp-proto");

const {
    PeriodicExportingMetricReader
} = require("@opentelemetry/sdk-metrics");



const traceExporter = new OTLPTraceExporter({
    url:
        process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ||
        "http://localhost:4318/v1/traces"
});

const metricExporter = new OTLPMetricExporter({
    url:
        process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT ||
        "http://localhost:4318/v1/metrics"
});

const metricReader = new PeriodicExportingMetricReader({
    exporter: metricExporter,
    exportIntervalMillis: 15000
});


const sdk = new NodeSDK({
    traceExporter,
    metricReader,
    instrumentations: [
        getNodeAutoInstrumentations()
    ]
});

sdk.start();

console.log("OpenTelemetry started");

process.on("SIGTERM", () => {
    sdk
        .shutdown()
        .then(() => console.log("OpenTelemetry shut down"))
        .catch((error) => console.error("Error shutting down OpenTelemetry", error))
        .finally(() => process.exit(0));
});