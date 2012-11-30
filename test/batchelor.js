var assert = require('assert');
var fs = require('fs');
var stream = require('stream');
var vows = require('vows');

var batchelor = require('../lib/batchelor');
var myutil = require('../lib/util');

var suite = vows.describe('batchelor').addBatch({
  'create a Batchelor factory': {
    topic: function() {
      var callback = this.callback;
      var factory = function(tableName) {
        var path = '/tmp/' + new Date().getTime() + '.' + tableName + '.test.batchelor';
        fs.mkdirSync(path);
        var b = batchelor.create(tableName, {interval: 10, path: path});
        var ctx = {batchelor: b,
                   path: path,
                   listFiles: function(cb) {
                     return fs.readdir(path, cb);
                   }};
        return ctx;
      };
      return callback(null, factory);
    },
    'verify it is readable/writable': function(err, factory) {
      var ctx = factory('test');
      var b = ctx.batchelor;

      assert.isTrue(b.readable);
      assert.isTrue(b.writable);
      assert.instanceOf(b, stream);
      assert.isFunction(b.write);
      assert.isFunction(b.end);
      assert.isFunction(b.pipe);
    },
    'write 100 rows, wait for 15ms and list the new files in the data path': {
      topic: function(err, factory) {
        var callback = this.callback;
        var ctx = factory('test');
        var b = ctx.batchelor;

        ctx.listFiles(function(err, origFiles) {
          if (err) {
            return callback(err);
          } else {
            for (var i = 0; i < 100; ++i) {
              b.write({hello: 'world', num: i});
            }
            setTimeout(function() {
              ctx.listFiles(function(err, files) {
                if (err) {
                  return callback(err);
                } else {
                  var newFiles = difference(origFiles, files);
                  newFiles = newFiles.map(function(file) {
                    return ctx.path + '/' + file;
                  });
                  return callback(null, newFiles, ctx);
                }
              });
            }, 15);
          }
        });
      },
      'verify there is a single, new data file': verifyNewDataFiles(1, new RegExp('test\.[0-9]+\.data')),
      'read the new file': {
        topic: function(err, newFiles, ctx) {
          var callback = this.callback;
          fs.readFile(newFiles[0], function(err, data) {
            if (err) {
              return callback(err);
            } else {
              return callback(null, data);
            }
          });
        },
        'verify the contents contains 100 lines of JSON data': verifyNLinesOfJsonData(100, function(obj) {
          assert.include(obj, 'table');
          assert.equal(obj.table, 'test');
          assert.include(obj, 'row');
          assert.include(obj.row, 'hello');
          assert.equal(obj.row.hello, 'world');
          assert.include(obj.row, 'num');
          assert.isNumber(obj.row.num);
          assert.isTrue(obj.row.num >= 0 && obj.row.num < 100);
        }),
      },
      'stat the file': {
        topic: function(err, newFiles, ctx) {
          var callback = this.callback;
          fs.stat(newFiles[0], function(err, stats) {
            return callback(err, stats);
          });
        },
        'verify it is a read only file': function(err, stats) {
          assert.isNull(err);
          assert.isTrue(stats.isFile());
          assert.equal(myutil.readablePermissions(stats.mode), '444');
        }
      }
    },
    'write 100 rows, wait for 15ms, write another 100, wait for 15ms and list the new files in the data path': {
      topic: function(err, factory) {
        var callback = this.callback;
        var ctx = factory('test2');
        var b = ctx.batchelor;

        ctx.listFiles(function(err, origFiles) {
          if (err) {
            return callback(err);
          } else {
            for (var i = 0; i < 100; ++i) {
              b.write({hello: 'world', num: i});
            }
            setTimeout(function() {
              for (var i = 100; i < 200; ++i) {
                b.write({hello: 'world2', num: i});
              }
              b.end();
              setTimeout(function() {
                ctx.listFiles(function(err, files) {
                  if (err) {
                    return callback(err);
                  } else {
                    var newFiles = difference(origFiles, files);
                    newFiles = newFiles.map(function(file) {
                      return ctx.path + '/' + file;
                    });
                    return callback(null, newFiles, ctx);
                  }
                });
              }, 15);
            }, 15);
          }
        });
      },
      'verify there are 2 new data files': verifyNewDataFiles(2, new RegExp('test2\.[0-9]+\.data')),
      'read the new files': {
        topic: function(err, newFiles, ctx) {
          var callback = this.callback;
          fs.readFile(newFiles[0], function(err, data) {
            if (err) {
              return callback(err);
            } else {
              fs.readFile(newFiles[1], function(err, data2) {
                if (err) {
                  return callback(err);
                } else {
                  return callback(null, data + '\n' + data2);
                }
              });
            }
          });
        },
        'verify the contents contains 200 lines of JSON data': verifyNLinesOfJsonData(200, function(obj) {
          assert.include(obj.row, 'num');
          assert.isNumber(obj.row.num);
          assert.include(obj, 'table');
          assert.equal(obj.table, 'test2');
          assert.include(obj, 'row');
          assert.include(obj.row, 'hello');
          assert.isTrue(obj.row.num >= 0 && obj.row.num < 200);

          var num = obj.row.num;
          if (num < 100) {
            assert.equal(obj.row.hello, 'world');
          } else {
            assert.equal(obj.row.hello, 'world2');
          }
        })
      }
    }
  }
}).export(module, {error: false});


function identityMap(arr) {
  var retObj = {};
  var i;
  var numElements = arr.length;
  var cur;
  for (i = 0; i < numElements; ++i) {
    cur = arr[i];
    retObj[cur] = cur;
  }
  return retObj;
}

function difference(arr1, arr2) {
  var a1 = identityMap(arr1);
  var a2 = identityMap(arr2);

  var k;
  var ret = [];
  for (k in a2) {
    if (!a1.hasOwnProperty(k)) {
      ret.push(k);
    }
  }
  return ret;
}

function verifyNewDataFiles(numExpected, matchRegex) {
  return function(err, newFiles, ctx) {
    assert.isNull(err);
    assert.isArray(newFiles);
    assert.lengthOf(newFiles, numExpected);

    var newFile = newFiles[0];
    assert.match(newFile, matchRegex);
  };
}

function verifyNLinesOfJsonData(numExpected, checkObjFn) {
  return function(err, fileData) {
    assert.isNull(err);

    var fileLines = fileData.toString().split('\n');
    fileLines = fileLines.filter(function(line) {
      return line.match(/^\{/);
    });
    assert.lengthOf(fileLines, numExpected);

    var i;
    var cur;
    for (i = 0; i < numExpected; ++i) {
      cur = fileLines[i]; 
      assert.doesNotThrow(function() {
        cur = JSON.parse(cur);
      }, Error);
      checkObjFn(cur);
    }
  };
};
