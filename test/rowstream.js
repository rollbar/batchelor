var assert = require('assert');
var stream = require('stream');
var vows = require('vows');

var batchelor = require('../lib/batchelor');
var rowstream = batchelor.rowstream;

var suite = vows.describe('rowstream').addBatch({
  'create a rowstream': {
    topic: function() {
      return rowstream.create('test');
    },
    'verify it is readable/writable': function(rs) {
      assert.isTrue(rs.readable);
      assert.isTrue(rs.writable);
      assert.instanceOf(rs, stream);
      assert.isFunction(rs.write);
      assert.isFunction(rs.end);
      assert.isFunction(rs.pipe);
    },
    'write a valid row': {
      topic: function(rs) {
        var callback = this.callback;
        rs.once('data', function(data) {
          return callback(null, data);
        });
        rs.once('error', function(err) {
          return callback(err);
        });

        rs.write({hello: 'world'});
        rs.end();
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
  },
  'create a rowstream factory': {
    topic: function() {
      return function() {
        return rowstream.create('test'); 
      };
    },
    'write a valid row using end(row)': {
      topic: function(factory) {
        var rs = factory();
        var callback = this.callback;

        var events = [];

        rs.once('data', function(data) {
          events.push('data');
        });
        rs.once('error', function(err) {
          events.push('error');
        });
        rs.once('end', function() {
          events.push('end');
          callback(events);
        });

        rs.end({hello: 'world'});
      },
      'verify order of fired events': function(events) {
        assert.lengthOf(events, 2);
        assert.equal(events[0], 'data');
        assert.equal(events[1], 'end');
      }
    },
    'write an invalid int row': {
      topic: function(factory) {
        var rs = factory();
        var callback = this.callback;
        rs.once('data', function(data) {
          return callback(null, data);
        });
        rs.once('error', function(err) {
          return callback(err);
        });

        rs.write(123);
        rs.end();
      },
      'verify the error event fires': function(err, data) {
        assert.isNotNull(err);
        assert.isUndefined(data);
      }
    },
    'write an invalid null row': {
      topic: function(factory) {
        var rs = factory();
        var callback = this.callback;
        rs.once('data', function(data) {
          return callback(null, data);
        });
        rs.once('error', function(err) {
          return callback(err);
        });

        rs.write(null);
        rs.end();
      },
      'verify the error event fires': function(err, data) {
        assert.isNotNull(err);
        assert.isUndefined(data);
      }
    },
    'write an invalid array row': {
      topic: function(factory) {
        var rs = factory();
        var callback = this.callback;
        rs.on('data', function(data) {
          return callback(null, data);
        });
        rs.on('error', function(err) {
          return callback(err);
        });

        rs.write([]);
        rs.end();
      },
      'verify the error event fires': function(err, data) {
        assert.isNotNull(err);
        assert.isUndefined(data);
      }
    },
    'write 2 rows with write then end': {
      topic: function(factory) {
        var rs = factory();
        var callback = this.callback;

        var rows = [];
        var errors = [];

        rs.on('data', function(data) {
          rows.push(data);
        });
        rs.once('error', function(err) {
          errors.push(err);
        });
        rs.once('end', function() {
          callback(rows, errors);
        });

        rs.write({test: 1});
        rs.end({test: 2});
      },
      'verify there are two rows': function(rows, errors) {
        assert.lengthOf(rows, 2);
        assert.lengthOf(errors, 0);
      },
      'verify the order of the rows': function(rows, errors) {
        var row1 = JSON.parse(rows[0]);
        var row2 = JSON.parse(rows[1]);
        assert.equal(row1.row.test, 1);
        assert.equal(row2.row.test, 2);
      }
    },
    'write 2 rows with write then write then end': {
      topic: function(factory) {
        var rs = factory();
        var callback = this.callback;

        var rows = [];
        var errors = [];

        rs.on('data', function(data) {
          rows.push(data);
        });
        rs.once('error', function(err) {
          errors.push(err);
        });
        rs.once('end', function() {
          callback(rows, errors);
        });

        rs.write({test: 1});
        rs.write({test: 2});
        rs.end();
      },
      'verify there are two rows': function(rows, errors) {
        assert.lengthOf(rows, 2);
        assert.lengthOf(errors, 0);
      },
      'verify the order of the rows': function(rows, errors) {
        var row1 = JSON.parse(rows[0]);
        var row2 = JSON.parse(rows[1]);
        assert.equal(row1.row.test, 1);
        assert.equal(row2.row.test, 2);
      }
    }
  }
}).export(module, {error: false});
