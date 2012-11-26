var assert = require('assert');
var fs = require('fs');
var stream = require('stream');
var vows = require('vows');

var batchelor = require('../lib/batchelor');
var LazyFileWriteStream = batchelor.LazyFileWriteStream;

var suite = vows.describe('lazyfilewritestream').addBatch({
  'create a LazyFileWriteStream': {
    topic: function() {
      var filename = '/tmp/' + Math.floor(new Date().getTime() / 1000) + 
        '.test.lazyfilewritestream';
      return this.callback(filename, new LazyFileWriteStream(filename, {flags: 'w'}));
    },
    'verify it is writeable': function(filename, ws) {
      assert.isFalse(ws.readable);
      assert.isTrue(ws.writeable);
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
