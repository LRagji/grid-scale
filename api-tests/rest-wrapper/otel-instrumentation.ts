import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';

// Start OpenTelemetry SDK before anything else so auto-instrumentation can hook into libs
const sdk = new NodeSDK({
    resource: resourceFromAttributes({
        [ATTR_SERVICE_NAME]: process.env.OTEL_SERVICE_NAME || "service-a",
        [ATTR_SERVICE_VERSION]: '1.0',
    }),
    instrumentations: [getNodeAutoInstrumentations()],
    // NodeSDK accepts traceExporter; it will wire a BatchSpanProcessor internally.
});

sdk.start();


// enable host metrics after calling start
// it'll automatically get the global `MeterProvider` set up by `sdk.start()` if you don't provide any options.
import { HostMetrics } from '@opentelemetry/host-metrics';
new HostMetrics().start();