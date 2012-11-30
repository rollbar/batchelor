var assert = require('assert');
var fs = require('fs');
var stream = require('stream');
var vows = require('vows');

var batchelor = require('../lib/batchelor');
var lazywritestream = batchelor.lazywritestream;

var suite = vows.describe('lazywritestream').addBatch({
  'create a lazy write stream': {
    topic: function() {
      var filename = '/tmp/' + Math.floor(new Date().getTime() / 1000) + 
        '.test.lazyfilewritestream';
      var wsFactory = function() {
        return fs.createWriteStream(filename, {flags: 'w'});
      };
      var lazy = lazywritestream.create(wsFactory);
      return this.callback(filename, lazy);
    },
    'verify it is writable': function(filename, ws) {
      assert.isFalse(ws.readable);
      assert.isTrue(ws.writable);
      assert.instanceOf(ws, stream);
      assert.isFunction(ws.write);
      assert.isFunction(ws.end);
      assert.isFunction(ws.pipe);
    },
    'stat the filename': {
      topic: function(filename, ws) {
        var callback = this.callback;
        fs.stat(filename, function(err, stats) {
          return callback(err, filename, ws);
        });
      },
      'verify it does not exist': function(err, filename, ws) {
        assert.isNotNull(err);
        assert.equal(err.code, 'ENOENT');
      },
      'write some data and read the file': {
        topic: function(err, filename, ws) {
          var callback = this.callback;
          ws.on('close', function() {
            fs.readFile(filename, function(err, data) {
              return callback(err, data);
            });
          });
          ws.end('HELLO WORLD'); 
        },
        'verify contents of the file': function(err, data) {
          assert.isNull(err);
          assert.equal(data, 'HELLO WORLD');
        }
      }
    }
  }
}).export(module, {error: false});
