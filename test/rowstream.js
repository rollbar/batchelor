var assert = require('assert');
var stream = require('stream');
var vows = require('vows');

var batchelor = require('../lib/batchelor');
var RowStream = batchelor.RowStream;

var suite = vows.describe('rowstream').addBatch({
  'create a RowStream': {
    topic: function() {
      return new RowStream('test'); 
    },
    'verify it is readable': function(rs) {
      assert.isTrue(rs.readable);
      assert.instanceOf(rs, stream);
      assert.isFunction(rs.write);
      assert.isFunction(rs.end);
    },
    'write a row': {
      topic: function(rs) {
        var callback = this.callback;
        rs.on('data', function(data) {
          return callback(null, data);
        });
        rs.on('error', function(err) {
          return callback(err);
        });

        rs.write({hello: 'world'});
      },
      'verify the data event fires': function(err, data) {
        assert.isNull(err);
        assert.isString(data);
      },
      'verify the serialized data ends with a newline': function(err, data) {
        assert.isNull(err);
        assert.equal(data[data.length - 1], '\n');
      },
      'verify the serialized data is JSON': function(err, data) {
        assert.isNull(err);
        assert.doesNotThrow(function() {
          JSON.parse(data);
        }, Error);
      },
      'verify the table name': function(err, data) {
        assert.isNull(err);
        var obj = JSON.parse(data);
        assert.strictEqual(obj.table, 'test');
      },
      'verify the row': function(err, data) {
        assert.isNull(err);
        var obj = JSON.parse(data);
        assert.include(obj, 'row');
        assert.include(obj.row, 'hello');
        assert.equal(obj.row.hello, 'world');
      }
    }

  }
}).export(module, {error: false});
