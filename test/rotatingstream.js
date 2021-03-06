var assert = require('assert');
var fs = require('fs');
var stream = require('stream');
var vows = require('vows');

var batchelor = require('../lib/batchelor');
var rotatingstream = batchelor.rotatingstream;
var lazywritestream = batchelor.lazywritestream;

var suite = vows.describe('rotatingstream').addBatch({
  'create a RotatingStream factory': {
    topic: function() {
      var callback = this.callback;
      
      var factory = function(suffix, maxSize) {
        var filename1 = '/tmp/' + new Date().getTime() + '.test.1.rotatingstream';
        var filename2 = '/tmp/' + new Date().getTime() + '.test.2.rotatingstream';

        if (suffix) {
          filename1 += suffix;
          filename2 += suffix;
        }

        var lazyStream1 = lazywritestream.create(function() {
          return fs.createWriteStream(filename1);
        });
        var lazyStream2 = lazywritestream.create(function() {
          return fs.createWriteStream(filename2);
        });
        var ctx = {filename1: filename1,
                   filename2: filename2,
                   stream1: lazyStream1,
                   stream2: lazyStream2,
                   rotateOnce: false,
                   numStreamFactoryCalls: 0,
                   numRotateCheckCalls: 0};

        var streamFactory = function() {
          ctx.numStreamFactoryCalls++;
          if (ctx.numStreamFactoryCalls % 2 === 0) {
            return lazyStream2;
          } else {
            return lazyStream1;
          }
        };
        var checkRotate = function(writeStream, callback) {
          ctx.numRotateCheckCalls++;
          if (ctx.rotateOnce) {
            ctx.rotateOnce = false;
            return callback(true);
          } else {
            return callback(false);
          }
        };

        var rs = rotatingstream.create(streamFactory, checkRotate, 10, maxSize); 
        ctx.rotateStream = rs;
        return ctx;
      };
      return this.callback(factory);
    },
    'verify it is readable/writable': function(factory) {
      var ctx = factory();
      var rs = ctx.rotateStream;

      assert.isTrue(rs.readable);
      assert.isTrue(rs.writable);
      assert.instanceOf(rs, stream);
      assert.isFunction(rs.write);
      assert.isFunction(rs.end);
      assert.isFunction(rs.pipe);
    },
    'verify only 1 streamFactory calls have been made': function(factory) {
      var ctx = factory();
      var rs = ctx.rotateStream;

      assert.equal(ctx.numStreamFactoryCalls, 1);
    },
    'verify no bytes have been written yet': function(factory) {
      var ctx = factory();
      var rs = ctx.rotateStream;

      assert.equal(rs.totalBytesWritten, 0);
    },
    'wait 105ms': {
      topic: function(factory) {
        var callback = this.callback;
        var ctx = factory();
        setTimeout(function() {
          return callback(ctx);
        }, 105);
      },
      'verify the rotateCheck function has been called at least 9 times': function(ctx) {
        // setTimeout looks like it's generally a couple of milliseconds behind
        assert.isTrue(ctx.numRotateCheckCalls >= 9);
      },
      'verify the current stream is stream1': function(ctx) {
        assert.strictEqual(ctx.rotateStream.currentStream, ctx.stream1);
        assert.equal(ctx.numStreamFactoryCalls, 1);
      }
    },
    'rotate the streams once': {
      topic: function(factory) {
        var callback = this.callback;
        var ctx = factory();
        ctx.rotateOnce = true;
        setTimeout(function() {
          return callback(ctx);
        }, 11);
      },
      'verify that the current stream is now stream2': function(ctx) {
        assert.strictEqual(ctx.rotateStream.currentStream, ctx.stream2);
        assert.equal(ctx.numStreamFactoryCalls, 2);
      }
    },
    'write to the rotate stream and wait for 15ms': {
      topic: function(factory) {
        var callback = this.callback;
        var ctx = factory();
        var rs = ctx.rotateStream;

        rs.write('HELLO WORLD');
        ctx.rotateOnce = true;
        setTimeout(function() {
          return callback(ctx);
        }, 15);
      },
      'verify the current stream is stream2': function(ctx) {
        assert.strictEqual(ctx.rotateStream.currentStream, ctx.stream2);
      },
      'verify stream1 is no longer writable': function(ctx) {
        assert.isFalse(ctx.stream1.writable);
      },
      'read the stream1 file': {
        topic: function(ctx) {
          var callback = this.callback;
          fs.readFile(ctx.filename1, function(err, data) {
            if (err) {
              return callback(err);
            } else {
              return callback(null, data, ctx);
            }
          });
        },
        'verify the contents of the stream1 file': function(err, data, ctx) {
          assert.isNull(err);
          assert.equal(data, 'HELLO WORLD');
        },
        'write to the rotate stream and wait another 11ms and read the stream2 file': {
          topic: function(err, data, ctx) {
            var callback = this.callback;
              ctx.rotateStream.write('HOLA EL MUNDO.');
            setTimeout(function() {
              fs.readFile(ctx.filename2, function(err, data) {
                if (err) {
                  return callback(err);
                } else {
                  return callback(null, data, ctx);
                }
              }); 
            }, 11);
          },
          'verify contents of the stream2 file': function(err, data, ctx) {
            assert.isNull(err);
            assert.equal(data, 'HOLA EL MUNDO.');
          },
          'write to the rotate stream so that it goes to stream2': {
            topic: function(err, data, ctx) {
              var callback = this.callback;
              ctx.rotateStream.write(' BUENAS DIAS.');
              fs.readFile(ctx.filename2, function(err, data) {
                if (err) {
                  return callback(err);
                } else {
                  return callback(null, data, ctx);
                }
              });
            },
            'verify contents of the stream2 file': function(err, data, ctx) {
              assert.isNull(err);
              assert.equal(data, 'HOLA EL MUNDO. BUENAS DIAS.');
            },
            'read in the stream1 file': {
              topic: function(err, data, ctx) {
                var callback = this.callback;
                fs.readFile(ctx.filename1, function(err, data) {
                  if (err) {
                    return callback(err);
                  } else {
                    return callback(null, data, ctx);
                  }
                });
              },
              'verify contents of the stream1 file': function(err, data, ctx) {
                assert.isNull(err);
                assert.equal(data, 'HELLO WORLD');
              }
            }
          }
        }
      }
    },
    'create a rotate stream that will rotate after 10 KB and write 20 KB to it': {
      topic: function(factory) {
        var callback = this.callback;
        var ctx = factory('.sizeLimit', 10 * 1024);
        var rotateStream = ctx.rotateStream;
        var buf = new Array(1024);
        var i;

        for (i = 0; i < 1023; ++i) {
          buf[i] = Math.floor((Math.random() * 10));
        }
        buf[i] = '\n';
        buf = buf.join('');
        for (i = 0; i < 20; ++i) {
          rotateStream.write(buf);
        }
        setTimeout(function() {
          return callback(ctx);
        }, 10);
      },
      'verify there are only 2 files written to': function(ctx) {
        assert.equal(ctx.numStreamFactoryCalls, 2); 
      },
      'stat the first file': {
        topic: function(ctx) {
          var callback = this.callback;
          fs.stat(ctx.filename1, function(err, stats) {
            if (err) {
              return callback(err);
            } else {
              return callback(null, stats);
            }
          });
        },
        'verify it is no larger than 10 KB': function(err, stats) {
          assert.isNull(err);
          assert.isTrue(stats.size && stats.size <= 10 * 1024);
        }
      },
      'stat the second file': {
        topic: function(ctx) {
          var callback = this.callback;
          fs.stat(ctx.filename2, function(err, stats) {
            if (err) {
              return callback(err);
            } else {
              return callback(null, stats);
            }
          });
        },
        'verify it is no larger than 10 KB': function(err, stats) {
          assert.isNull(err);
          assert.isTrue(stats.size && stats.size <= 10 * 1024);
        }
      }
    }
  }
}).export(module, {error: false});
