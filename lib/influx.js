/**
 * @fileOverview influx helper.
 *
 * @author       <a href="mailto:sjian@microsoft.com">Arthur Jiang</a>
 * @version      1.0.0
 *
 * @requires     influx
 * @requires     ./log,../config,../redis
 * @module       influx
 */

const config = require(`${__dirname}/../config`);
const Log = require(`${__dirname}/log`);
const pubsub = require(`${__dirname}/pubsub`);

const maxInterval = 1200;
const minInterval = 600;
const maxDataPointQueueSize = 100000;
const dataPoints = {};
const handlers = {};
let currReq = 0;
let maxReq = 10;
const minReq = 1;
const minDataPointNum = 10;
const maxDataPointNum = 600;
const log = new Log({
  tag: 'lib_influx'
});

const _interval = (handler, handlerKey) => {
  return () => {
    if (currReq < maxReq) {
      if (dataPoints[handlerKey]['queue'].length > 0) {
        currReq++;
        let splicedDataPoints = dataPoints[handlerKey]['queue'].splice(0, dataPoints[handlerKey]['max_datapoint_num']);

        handler.writePoints(splicedDataPoints)
          .then((res) => {
            currReq--;
            maxReq = currReq * 2 < minReq ? minReq * 10 : currReq * 2;

            if (Math.floor(dataPoints[handlerKey]['max_datapoint_num'] * 1.1) > maxDataPointNum * 1.5) {
              dataPoints[handlerKey]['max_datapoint_num'] += 10;
            } else {
              dataPoints[handlerKey]['max_datapoint_num'] = Math.floor(dataPoints[handlerKey]['max_datapoint_num'] * 1.1);
            }

            if (dataPoints[handlerKey]['max_datapoint_num'] > dataPoints[handlerKey]['queue'].length) {
              dataPoints[handlerKey]['max_datapoint_num'] = Math.max(Math.floor(dataPoints[handlerKey]['queue'].length * 1.2),
                                                                     minDataPointNum);
            }

            dataPoints[handlerKey]['success'] += 1;
            _formatLog('insert data success', res,
                       currReq, maxReq, dataPoints[handlerKey]['max_datapoint_num'],
                       dataPoints[handlerKey]['queue'].length, dataPoints[handlerKey]['retry_queue'].length,
                       dataPoints[handlerKey]['success'], dataPoints[handlerKey]['fail'],
                       dataPoints[handlerKey]['interval']);
          })
          .catch((e) => {
            dataPoints[handlerKey]['retry_queue'] = dataPoints[handlerKey]['retry_queue'].concat(splicedDataPoints);
            currReq--;
            maxReq = Math.ceil(maxReq / 2) < minReq ? minReq : Math.ceil(maxReq / 2);
            dataPoints[handlerKey]['max_datapoint_num'] = Math.ceil(dataPoints[handlerKey]['max_datapoint_num'] / 2) < minDataPointNum ? minDataPointNum : Math.ceil(dataPoints[handlerKey]['max_datapoint_num'] / 2);
            dataPoints[handlerKey]['fail'] += 1;
            _formatLog('insert data error', e,
                       currReq, maxReq, dataPoints[handlerKey]['max_datapoint_num'],
                       dataPoints[handlerKey]['queue'].length, dataPoints[handlerKey]['retry_queue'].length,
                       dataPoints[handlerKey]['success'], dataPoints[handlerKey]['fail'],
                       dataPoints[handlerKey]['interval']);
          });
      } else if (dataPoints[handlerKey]['retry_queue'].length > 0) {
        currReq++;
        let splicedDataPoints = dataPoints[handlerKey]['retry_queue'].splice(0, dataPoints[handlerKey]['max_datapoint_num']);

        handler.writePoints(splicedDataPoints)
          .then((res) => {
            currReq--;
            maxReq = currReq * 2 < minReq ? minReq * 10 : currReq * 2;

            if (Math.floor(dataPoints[handlerKey]['max_datapoint_num'] * 1.05) > maxDataPointNum * 1.5) {
              dataPoints[handlerKey]['max_datapoint_num'] += 5;
            } else {
              dataPoints[handlerKey]['max_datapoint_num'] = Math.floor(dataPoints[handlerKey]['max_datapoint_num'] * 1.05);
            }

            if (dataPoints[handlerKey]['max_datapoint_num'] > dataPoints[handlerKey]['retry_queue'].length) {
              dataPoints[handlerKey]['max_datapoint_num'] = Math.max(Math.floor(dataPoints[handlerKey]['retry_queue'].length * 1.1),
                                                                     minDataPointNum);
            }

            dataPoints[handlerKey]['success'] += 1;
            _formatLog('retry insert data success', res,
                       currReq, maxReq, dataPoints[handlerKey]['max_datapoint_num'],
                       dataPoints[handlerKey]['queue'].length, dataPoints[handlerKey]['retry_queue'].length,
                       dataPoints[handlerKey]['success'], dataPoints[handlerKey]['fail'],
                       dataPoints[handlerKey]['interval']);
          })
          .catch((e) => {
            pubsub.pub(`${handlerKey}:retry_fail`, splicedDataPoints);
            currReq--;
            maxReq = Math.ceil(maxReq / 2) < minReq ? minReq : Math.ceil(maxReq / 2);
            dataPoints[handlerKey]['max_datapoint_num'] = Math.ceil(dataPoints[handlerKey]['max_datapoint_num'] / 2) < minDataPointNum ? minDataPointNum : Math.ceil(dataPoints[handlerKey]['max_datapoint_num'] / 2);
            dataPoints[handlerKey]['fail'] += 1;
            _formatLog('retry insert data error', e,
                       currReq, maxReq, dataPoints[handlerKey]['max_datapoint_num'],
                       dataPoints[handlerKey]['queue'].length, dataPoints[handlerKey]['retry_queue'].length,
                       dataPoints[handlerKey]['success'], dataPoints[handlerKey]['fail'],
                       dataPoints[handlerKey]['interval']);
          });
      }
    }
  };
};

const _formatLog = (action, state, curReq, maxReq, bathSize, queueSize, retryQueueSize, successNum, failNum, curInterval) => {
  log.info(`------------------------------------------------`);
  log.info(`${action}: ${state}`);
  log.info(`cur reqs: ${curReq}`);
  log.info(`max reqs: ${maxReq}`);
  log.info(`bath size: ${bathSize}`);
  log.info(`queue size: ${queueSize}`);
  log.info(`retry queue size: ${retryQueueSize}`);
  log.info(`success num: ${successNum}`);
  log.info(`fail num: ${failNum}`);
  log.info(`cur interval: ${curInterval}`);
  log.info(`approximately memory: ${Math.round(process.memoryUsage().heapUsed/1024/1024 * 100)/100} MB`);
  log.info(`------------------------------------------------`);
};

const errorCode = {
  queue_full: 1,
  retry_fail: 2
};

/**
 * Write data point to influx - has an internal buffer for request scheduler
 * @param {string} handlerKey - handler key name, which you defined in config file
 * @param {object} dataPoint - data point object
 * @param {function} cb - call back, success only means inqueue(buffer) success.
 * error case:
 * 1. create db error
 * 2. network error
 * 3. meet internal queue threshold: queue_full error
 * 4. retry error: need use pubsub.sub(`${handlerKey}:retry_fail`, your_handler).
 * @example
 * let dataPoint = {
 *  measurement: 'scheduler',
 *  tags: {
 *    job: 'test',
 *    host: '127.0.0.1',
 *    pid: process.pid,
 *    timezone: 'utf8'
 *  },
 *  fields: {
 *    err: 'null',
 *    res: 'test'
 *  },
 *  timestamp: new Date().getTime() * 1000000
 * };
 *
 * influx.write(`${handlerKey}`, dataPoint, (err, res) => {
 *   if (err.code == influx.errorCode.queue_full) {
 *     ...
 *   }
 *   ...
 * });
 *
 * pubsub.sub(`${handlerKey}:retry_fail`, (data) => {
 *   ...
 * });
 */
const write = (handlerKey, dataPoint, cb) => {
  let handler = process.env.NODE_ENV == 'development' ? config.influx.test.handlers[handlerKey] : config.influx.pro.handlers[handlerKey];
  let table = process.env.NODE_ENV == 'development' ? config.influx.test.table[handlerKey] : config.influx.pro.table[handlerKey];
  let host = process.env.NODE_ENV == 'development' ? config.influx.test.host : config.influx.pro.port;
  let port = process.env.NODE_ENV == 'development' ? config.influx.test.port : config.influx.pro.port;
 
  if (dataPoints[handlerKey] === undefined) {
    dataPoints[handlerKey] = {};
    dataPoints[handlerKey]['queue'] = [];
    dataPoints[handlerKey]['retry_queue'] = [];
    dataPoints[handlerKey]['interval'] = maxInterval;
    dataPoints[handlerKey]['max_datapoint_num'] = maxDataPointNum;
    dataPoints[handlerKey]['success'] = 0;
    dataPoints[handlerKey]['fail'] = 0;

    log.info(`Create database ${table} in ${host}:${port} ...`);
    handler.createDatabase(table)
      .then((res) => {
        log.info(`Create database ${table} in ${host}:${port} successfully`);

        dataPoints[handlerKey]['setInterval'] = setInterval(_interval(handler, handlerKey), dataPoints[handlerKey]['interval']);

        dataPoints[handlerKey]['updateInterval'] = setInterval(() => {
          if (dataPoints[handlerKey]['fail'] == 0 && dataPoints[handlerKey]['success'] > 1) {
            dataPoints[handlerKey]['interval'] = dataPoints[handlerKey]['interval'] - 10 < minInterval ? minInterval : dataPoints[handlerKey]['interval'] - 10;
            clearInterval(dataPoints[handlerKey]['setInterval']);
            dataPoints[handlerKey]['setInterval'] = setInterval(_interval(handler, handlerKey), dataPoints[handlerKey]['interval']);
            _formatLog('upgrade interval', 'success',
                       currReq, maxReq, dataPoints[handlerKey]['max_datapoint_num'],
                       dataPoints[handlerKey]['queue'].length, dataPoints[handlerKey]['retry_queue'].length,
                       dataPoints[handlerKey]['success'], dataPoints[handlerKey]['fail'],
                       dataPoints[handlerKey]['interval']);
          } else if ((dataPoints[handlerKey]['fail'] / (dataPoints[handlerKey]['fail'] + dataPoints[handlerKey]['success'] + 1)) > 0.5) {
            dataPoints[handlerKey]['interval'] = dataPoints[handlerKey]['interval'] * 2 > 100000 ? 100000 : dataPoints[handlerKey]['interval'] * 2;
            clearInterval(dataPoints[handlerKey]['setInterval']);
            dataPoints[handlerKey]['setInterval'] = setInterval(_interval(handler, handlerKey), dataPoints[handlerKey]['interval']);
            _formatLog('downgrade interval', 'success',
                       currReq, maxReq, dataPoints[handlerKey]['max_datapoint_num'],
                       dataPoints[handlerKey]['queue'].length, dataPoints[handlerKey]['retry_queue'].length,
                       dataPoints[handlerKey]['success'], dataPoints[handlerKey]['fail'],
                       dataPoints[handlerKey]['interval']);
          }

          dataPoints[handlerKey]['success'] = 0;
          dataPoints[handlerKey]['fail'] = 0;

        }, 2000);
      })
      .catch((e) => {
        log.info(`Create database ${table} in ${hanler.host}:${handler.port} err: ${e}`);
        cb(e);
      });
  }

  if (dataPoints[handlerKey]['queue'].length + dataPoints[handlerKey]['retry_queue'].length >= maxDataPointQueueSize) {
    const err = new Error(`Current queue length meet the ${maxDataPointQueueSize} threshold`);
    err.code = errorCode.queue_full;
    cb(err);
  }

  dataPoints[handlerKey]['queue'].push(dataPoint);
  cb(null, `data point in queue successfully`);
};

/**
 * Read data point from influx
 * @param {string} handlerKey - handler key name
 * @param {object} dataPoint - data point object
 * @param {function} cb - call back
 * @example
 * influx.read(`${handlerKey}`, `select * from ${measurement} limit 10`, (err, res) => { ... });
 */
const read = (handlerKey, queryStr, cb) => {
  let handler = process.env.NODE_ENV == 'development' ? config.influx.test.handlers[handlerKey] : config.influx.pro.handlers[handlerKey];

  handler.query(queryStr)
    .then(results => {
      cb(null, results);
    })
    .catch((err) => {
      cb(err);
    });
};

/**
 * state - get topic state
 * @param {string} handlerKey - handlerKey name
 */
const state = (handlerKey) => {
  return {
    new_queue_size: dataPoints[handlerKey]['queue'].length,
    retry_queue_size: dataPoints[handlerKey]['retry_queue'].length
  };
};

/**
 * Read data point from influx, return raw data
 * @param {string} handlerKey - handler key name
 * @param {object} dataPoint - data point object
 * @param {function} cb - call back
 * @example
 * influx.read(`${handlerKey}`, `select * from ${measurement} limit 10`, (err, res) => { ... });
 */
const readRaw = (handlerKey, queryStr, cb) => {
  let handler = process.env.NODE_ENV == 'development' ? config.influx.test.handlers[handlerKey] : config.influx.pro.handlers[handlerKey];

  handler.queryRaw(queryStr)
    .then(results => {
      cb(null, results);
    })
    .catch((err) => {
      cb(err);
    });
};

/**
 * Drop db in influx
 * @param {string} handlerKey - handler key name
 * @param {function} cb - call back
 * @example
 * influx.dropDB(`${handlerKey}`, (err, res) => { ... });
 */
const dropDB = (handlerKey, cb) => {
  let handler = process.env.NODE_ENV == 'development' ? config.influx.test.handlers[handlerKey] : config.influx.pro.handlers[handlerKey];
  let table = process.env.NODE_ENV == 'developmet' ? config.influx.test.table[handlerKey] : config.influx.pro.table[handlerKey];

  handler.dropDatabase(table)
    .then((res) => {
      cb(null, res);
    })
    .catch((e) => {
      cb(e);
    });
};

/**
 * Clear intervals - call it when you want to stop handler keep try to write data
 * @param {string} handlerKey - handler key name
 */
const clearHandler = (handlerKey) => {
  clearInterval(dataPoints[handlerKey]['setInterval']);
  clearInterval(dataPoints[handlerKey]['updateInterval']);
};

module.exports = {
  write: write,
  read: read,
  readRaw: readRaw,
  dropDB: dropDB,
  clearHandler: clearHandler,
  errorCode: errorCode,
  state: state
};
