const { createLogger, format, transports } = require('winston');
const { combine, splat, timestamp, printf } = format;

const customFormatter = printf(({ level, message, timestamp, ...metadata }) => {
  let formattedMetadata = '';

  if (metadata && Object.keys(metadata).length > 0) {
    formattedMetadata = `\n${JSON.stringify(metadata, null, 2)}`;
  }

  const msg = `${timestamp} [${level}] : ${message}${formattedMetadata}`;
  return msg;
});

const logger = createLogger({
  level: 'debug',
  format: combine(
    format.colorize(),
    splat(),
    timestamp(),
    customFormatter),
  transports: [
    new transports.Console({ level: 'info' }),
  ]
});

module.exports = logger