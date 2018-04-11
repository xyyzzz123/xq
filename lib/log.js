/**
 * @fileOverview log helper, internal use debug and fluent-logger
 * This log support both local log and remote log, use remote function for fluent-logger
 * For local log, support trac, debug, info, warn, error, fatal levels, different level will have different color.
 *
 * @author       <a href="mailto:sjian@microsoft.com">Arthur Jiang</a>
 * @version      1.0.0
 * @module       log
 */
const debug = require('debug');
const chalk = require('chalk');
const os = require('os');
const config = require(`${__dirname}/../config`);
const Influx = require('influx');
const logger = require('fluent-logger');
logger.configure(`${process.env.NODE_ENV == 'development' ? config.fluentd.test.logger_name : config.fluentd.pro.logger_name}`, {
  host: `${process.env.NODE_ENV == 'development' ? config.fluentd.test.host : config.fluentd.pro.host}`,
  port: `${process.env.NODE_ENV == 'development' ? config.fluentd.test.port : config.fluentd.pro.port}`,
  timeout: 3.0,
  reconnectInterval: 600000
});

const usr = process.env.USER;
const colDebug = process.env.COL_DEBUG;
const isColor = process.env.IS_COLOR == 'true';
const isRemote = process.env.SEND_REMOTE == 'true';

const logColor = {
  T: chalk.blue,
  D: chalk.white.bgBlack,
  I: chalk.green,
  W: chalk.yellow,
  E: chalk.red,
  F: chalk.red.bgYellow.bold
};

 /**
  * COL_DEBUG -- column debug filter, when * all column will be printed, support columns: tag, level, session, instance, scope.
  * IS_COLOR -- enable color or not, different log level will have different color, trace -> blue, debug -> background black,
  * info -> green, warn -> yellow, error -> red, fatal -> red, background yellow, bold.
  * @example
  * // create a log instance
  * const log = new Log({
  *     tag: 'test',
  *     instance: '${pid}',
  *     scope: 'i',
  *     session: 'test'
  *  });
  * log.info({k1: 1, k2: 2});
  *
  * // show all columns
  * DEBUG=test COL_DEBUG=* node yourfile.js
  *
  * // show tag,level column
  * DEBUG=test COL_DEBUG=tag,level node yourfile.js
  *
  * // only show message
  * DEBUG=test node yourfile.js
  *
  * // enable color
  * DEBUG=test IS_COLOR=true node yourfile.js
  *
  */
class Log {
  /**
   * constructor function
   * @param {obj} options - support tag, instance, scope, session.
   * default tag: `log`.
   * default instance: `${host}:${pid}`.
   * default scope: `g`.
   * default session: `${timestamp}`.
   * Local log format: tag level session:instance:scope msg
   */
  constructor(options) {
    this.tag = (options && options.tag) || `log`;
    this.instance = (options && options.instance) || `${os.hostname()}.${process.pid}`;
    this.scope = (options && options.scope) || `g`;
    this.session = (options && options.session) || `${new Date().getTime()}`;

    if (!isRemote) {
      this.locDebug = debug(this.tag);
    }

    this._nomalize();
  }

  /**
   * trace
   * @param {args} - obj1 ... objN, msg, subst1 ... substN, same to console.log
   * @example
   * // trace log
   * const log = new Log({
   *   tag: 'trace'
   * });
   *
   * log.trace('hi');
   */
  trace() {
    this._dispatch('T', arguments);
  }

  /**
   * debug
   * @param {args} - obj1 ... objN, msg, subst1 ... substN, same to console.log
   * @example
   * // debug log
   * const log = new Log({
   *   tag: 'debug'
   * });
   *
   * log.debug('hi');
   */
  debug() {
    this._dispatch('D', arguments);
  }

  /**
   * info
   * @param {args} - obj1 ... objN, msg, subst1 ... substN, same to console.log
   * @example
   * // infomation log
   * const log = new Log({
   *  tag: 'info'
   * });
   *
   * log.info('hi');
   */
  info() {
    this._dispatch('I', arguments);
  }

  /**
   * warn
   * @param {args} - obj1 ... objN, msg, subst1 ... substN, same to console.log
   * @example
   * // warning log
   * const log = new Log({
   *   tag: 'warning'
   * });
   *
   * log.warn('hi');
   */
  warn() {
    this._dispatch('W', arguments);
  }

  /**
   * error
   * @param {args} - obj1 ... objN, msg, subst1 ... substN, same to console.log
   * @example
   * // error log
   * const log = new Log({
   *   tag: 'error'
   * });
   *
   * log.error('hi');
   */
  error() {
    this._dispatch('E', arguments);
  }

  /**
   * fatal
   * @param {args} - obj1 ... objN, msg, subst1 ... substN, same to console.log
   * @example
   * // fatal log
   * const log = new Log({
   *   tag: 'fatal'
   * });
   *
   * log.fatal('hi');
   */
  fatal() {
    this._dispatch('F', arguments);
  }

  /**
   * remote - emit log to fluentd
   * @param {object} obj - kv object
   * @example
   * // send remote log
   * const log = new Log({
   *   tag: 'test'
   * });
   *
   * log.remote({k1: 1, k2: 2});
   */
  remote(obj) {
    logger.emit(this.tag, obj);
  }

  _nomalize() {
    this.tag = this.tag.toString().replace(/\s+/, '_');
    this.instance = this.instance.toString().replace(/\s+/, '_');
    this.scope = this.scope.toString().replace(/\s+/, '_');
    this.session = this.session.toString().replace(/\s+/, '_');
  }

  _dispatch(level, args) {
    let msg = Array.from(args).map(arg => typeof arg == 'object' ? JSON.stringify(arg) : arg).join(' ');

    let formattedMsg = this._formatter(level, msg);

    if (colDebug != '*') {
      formattedMsg = this._cutter(level, msg);
    }

    if (isColor) {
      this.locDebug(`${logColor[level](`${formattedMsg}`)}`);
    } else {
      this.locDebug(`${formattedMsg}`);
    }
  }

  _blackHole() {
    return;
  }

  _formatter(level, meta) {
    return [this.tag, level, [this.session, this.instance, this.scope].join(':'), meta].join(' ');
  }

  _cutter(level, meta) {
    if (colDebug) {
      return [
        colDebug.includes('tag') ? this.tag : '',
        colDebug.includes('level') ? level : '',
        [
          colDebug.includes('session') ? this.session : '',
          colDebug.includes('instance') ? this.instance : '',
          colDebug.includes('scope') ? this.scope : ''
        ].join(':'),
        meta
      ].join(' ');
    } else {
      return meta;
    }
  }
}

module.exports = Log;
