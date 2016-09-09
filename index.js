/**
 * @module kad-memstore
 */

'use strict';

var ReadableStream = require('readable-stream');
var assert = require('assert');
var _ = require('lodash');
var setImmediate = require('async.util.setimmediate');

/**
 * Creates an in-memory kad storage adapter
 * @constructor
 */
function KadMemStore() {
  if (!(this instanceof KadMemStore)) {
    return new KadMemStore();
  }

  this._store = {};
}

/**
 * Gets an item from the store
 * #get
 * @param {string} key
 * @param {function} callback
 */
KadMemStore.prototype.get = function(key, callback) {
  assert(_.isString(key), 'key is not a valid string')
  assert(_.isFunction(callback), 'callback is not a valid function')
  var self = this;
  setImmediate(function() {
    if(self._store[key]) {
      callback(null, self._store[key])
    } else {
      callback(new Error('Key not found'))
    }
  });
};

/**
 * Puts an item into the store
 * #put
 * @param {string} key
 * @param {string} value
 * @param {function} callback
 */
KadMemStore.prototype.put = function(key, value, callback) {
  assert(_.isString(key), 'key is not a valid string')
  assert(!callback || _.isFunction(callback), 'callback is not a valid function')
  assert(_.isString(value), 'value is not a valid string')
  this._store[key] = value;
  if(callback) {
    setImmediate(callback);
  }
};

/**
 * Deletes an item from the store
 * #del
 * @param {string} key
 * @param {function} callback
 */
KadMemStore.prototype.del = function(key, callback) {
  assert(_.isString(key), 'key is not a valid string')
  assert(!callback || _.isFunction(callback), 'callback is not a valid function')
  if(!callback) {
    callback = function() {}
  }
  setImmediate(function() {
    if(this._store[key]) {
      delete this._store[key]
      callback()
    } else {
      callback(new Error('Key not found'))
    }
  })
};

/**
 * Returns a readable stream of items
 * #createReadStream
 */
KadMemStore.prototype.createReadStream = function() {
  var adapter = this;
  var items = Object.keys(this._store);
  var current = 0;

  return new ReadableStream({
    objectMode: true,
    read: function() {
      var stream = this;
      var key = items[current];

      if (!key) {
        return stream.push(null);
      }

      setImmediate(function pushItem() {
        current++;
        stream.push({ key: key, value: adapter._store[key] });
      });
    }
  });
};

module.exports = KadMemStore;
