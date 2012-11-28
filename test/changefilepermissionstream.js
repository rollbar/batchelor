var assert = require('assert');
var fs = require('fs');
var stream = require('stream');
var vows = require('vows');

var myutil = require('../lib/util');

var batchelor = require('../lib/batchelor');
var ChangeFilePermissionStream = batchelor.ChangeFilePermissionStream;

var suite = vows.describe('changefilepermissionstream').addBatch({
  'write a file with 666 permissions': {
    topic: function() {
      var callback = this.callback;
      var filename = '/tmp/' + Math.floor(new Date().getTime() / 1000) +
        '.test.1.changefilepermissionstream';
      fs.writeFile(filename, '"the mark of the beast"', function(err) {
        if (err) {
          return callback(err);
        } else {
          fs.chmod(filename, '666', function(err) {
            if (err) {
              return callback(err);
            } else {
              return callback(null, filename);
            }
          });
        }
      });
    }, 
    'create a new read-only ChangeFilePermissionStream and send it the 666 file then stat it again': {
      topic: function(err, filename) {
        var callback = this.callback;
        var ws = new ChangeFilePermissionStream('444');
        ws.on('data', function(data) {
          fs.stat(filename, function(err, stats) {
            return callback(err, stats);
          });
        });
        ws.on('error', function(err) {
          return callback(err);
        });
        ws.end(filename);
      },
      'verify it has 444 permissions': function(err, stats) {
        assert.isNull(err);
        assert.isObject(stats);
        assert.equal(myutil.readablePermissions(stats.mode), '444');
      }
    }
  },
  'write a file with 777 permissions': {
    topic: function() {
      var callback = this.callback;
      var filename = '/tmp/' + Math.floor(new Date().getTime() / 1000) +
        '.test.2.changefilepermissionstream';
      fs.writeFile(filename, '"the mark of the beast"', function(err) {
        if (err) {
          return callback(err);
        } else {
          fs.chmod(filename, '777', function(err) {
            if (err) {
              return callback(err);
            } else {
              return callback(null, filename);
            }
          });
        }
      });
    }, 
    'create a new 644 ChangeFilePermissionStream and send it the 777 file then stat it again': {
      topic: function(err, filename) {
        var callback = this.callback;
        var ws = new ChangeFilePermissionStream('644');
        ws.on('data', function(data) {
          fs.stat(filename, function(err, stats) {
            return callback(err, stats);
          });
        });
        ws.on('error', function(err) {
          return callback(err);
        });
        ws.end(filename);
      },
      'verify it has 644 permissions': function(err, stats) {
        assert.isNull(err);
        assert.isObject(stats);
        assert.equal(myutil.readablePermissions(stats.mode), '644');
      }
    }
  }
}).export(module, {error: false});
