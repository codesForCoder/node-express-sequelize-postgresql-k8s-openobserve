const { Kafka } = require("kafkajs");
const logger = require("../config/logger");
const {
    context,
    propagation,
    trace,
    SpanKind,
    ROOT_CONTEXT,
} = require("@opentelemetry/api");
const {updateViaKafka} = require("../controllers/tutorial.controller");

const kafka = new Kafka({
    clientId: "node-tutorial-consumer",
    brokers: (
        process.env.KAFKA_BROKERS ||
        "localhost:29092"
    ).split(","),
});

const consumer = kafka.consumer({
    groupId:
        process.env.KAFKA_GROUP_ID ||
        "node-tutorial-consumer",
});

const tracer = trace.getTracer("node-tutorial-kafka");

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/*
 * KafkaJS headers are Buffer values.
 * OpenTelemetry propagation expects string-like values.
 */
const kafkaHeaderGetter = {
    get(carrier, key) {
        const value = carrier[key];

        if (value === undefined || value === null) {
            return undefined;
        }

        return Buffer.isBuffer(value)
            ? value.toString("utf8")
            : String(value);
    },

    keys(carrier) {
        return Object.keys(carrier);
    },
};

async function startKafkaConsumer() {
    await consumer.connect();

    await consumer.subscribe({
        topic: "app-events",
        fromBeginning: false,
    });

    logger.info("Kafka Tutorial consumer started .......");

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const headers = message.headers || {};
            logger.info(`message received from topic ${topic}`);
            /*
             * Extract the producer's trace context from Kafka headers.
             */
            const extractedContext = propagation.extract(
                ROOT_CONTEXT,
                headers,
                kafkaHeaderGetter
            );

            const producerSpanContext =
                trace.getSpanContext(extractedContext);
            const eventType = headers["event-type"]?.toString();
            /*
             * We create a NEW trace for the consumer.
             *
             * The producer context is NOT the parent.
             * Instead, it becomes a span LINK.
             */
            const links = trace.isSpanContextValid(producerSpanContext)
                ? [
                    {
                        context: producerSpanContext,
                        attributes: {
                            "messaging.kafka.event_type": eventType || "unknown",
                        },
                    },
                ]
                : [];

            const span = tracer.startSpan(
                "kafka.consume app-events",
                {
                    kind: SpanKind.CONSUMER,
                    links,
                    attributes: {
                        "messaging.system": "kafka",
                        "messaging.destination.name": topic,
                        "messaging.kafka.partition": partition,
                        "messaging.kafka.offset": message.offset,
                        "messaging.kafka.event_type": eventType || "unknown",
                    },
                },
                ROOT_CONTEXT
            );

            /*
             * Make our NEW consumer span the current context.
             * This is important so Sequelize spans created during
             * processing become children of this consumer span.
             */
            await context.with(
                trace.setSpan(ROOT_CONTEXT, span),
                async () => {
                    try {
                        logger.info(
                            `Kafka event received: type=${eventType}, offset=${message.offset}`
                        );

                        const event = JSON.parse(
                            message.value.toString("utf8")
                        );

                        logger.info(
                            `Processing Tutorial event :: ${JSON.stringify(event)}`
                        );
                        let id = event.recordId
                        let tutorial = {
                           title : event?.data?.title,
                            description : event?.data?.description,
                            published : event?.data?.published || false,
                        }
                        await updateViaKafka(id, tutorial);
                        logger.info(
                            `Processing Of Id ${id} completed for Tutorial ${JSON.stringify(tutorial)}`
                        );

                        span.setAttribute(
                            "kafka.processing.completed",
                            true
                        );
                    } catch (error) {
                        span.recordException(error);
                        span.setStatus({
                            code: 2,
                            message: error.message,
                        });
                    } finally {
                        span.end();
                    }
                }
            );
        },
    });
}

module.exports = {
    startKafkaConsumer,
};