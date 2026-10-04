const pino = require("pino");

const logger = pino({
    level: process.env.LOG_LEVEL || "info",
    base: {
        service: process.env.OTEL_SERVICE_NAME || "node-express-api"
    },
    timestamp: pino.stdTimeFunctions.isoTime
});

module.exports = logger;