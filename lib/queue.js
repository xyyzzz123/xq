/**
 * @fileOverview queue helper, a simple in-memory queue, only support basic in/out queue
 *
 * @author       <a href="mailto:sjian@microsoft.com">Arthur Jiang</a>
 * @version      1.0.0
 * @module       queue
 */

/**
 * @example
 * // in/out queue
 * queue.in('queue', 1);
 * assert(queue.out('queue') === 1);
 */
const Queue = {
  _queues: {},
  in: function (queue, item) {
    if (!this._queues[queue]) {
      this._queues[queue] = [];
    }

    this._queues[queue].push(item);
  },
  out: function (queue) {
    if (!this._queues[queue]) {
      return null;
    }

    return this._queues[queue].shift();
  }
};

module.exports = Queue;
