var eventstream = require('event-stream');
var fs = require('fs');
var util = require('util');

var rowstream = require('./stream/rowstream');
var rotatingstream = require('./stream/rotatingstream');
var lazywritestream = require('./stream/lazywritestream');

/*
 * Batchelor is a system for creating batch files from stream data
 * which can be used to batch insert/update a data source.
 * 
 * Usage:
 *
 *  var batchelor = require('batchelor');
 *  var auditEvent = new batchelor.Batchelor('audit_event');
 *  auditEvent.write({timestamp: 12345, eventName: 'pageview', id: 54321});
 *  auditEvent.end();
 *
 * The above example will create a new file in the current directory
 * called "audit_event.TIMESTAMP.data" which will contain 1 line for
 * every call to batchelor.write(). The line will be serialized JSON
 * in the format:
 *
 * {tableName: 'audit_event', row: {timestamp: TS, eventName: NAME, id: ID}}
 * 
 * You can then use this file to later perform a batch insert/update into
 * a database/sink of your choice.
 *
 * Files are set to read-only permissions, (444) once they are no longer being
 * written to.
 *
 */

exports.create = function(tableName, options) {
  options = options || {};

  var basePath = options.path || __dirname;
  var rotateInterval = options.interval === undefined ? 5000 : options.interval;

  var streamFactory = createStreamFactory(tableName, basePath, 'utf-8');
  var checkRotate = createCheckRotate(tableName, basePath);

  var pipeline = eventstream.pipeline(
    rowstream.create(tableName),
    rotatingstream.create(streamFactory, checkRotate, rotateInterval));

  pipeline.on('close', function() {
    pipeline.readable = false;
    pipeline.writable = false;
  });

  pipeline.readable = true;
  pipeline.writable = true;

  return pipeline;
};

exports.lazywritestream = lazywritestream;
exports.rotatingstream = rotatingstream;
exports.rowstream = rowstream;


/**** Internal ****/

var curFilenames = {};
function createStreamFactory(tableName, basePath, encoding) {
  var wsFactory = function() {
    var curFilename = datafileName(tableName, basePath);
    curFilenames[tableName] = curFilename;
    var ws = fs.createWriteStream(curFilename, {flags: 'a', encoding: encoding});
    ws.on('close', function() {
      fs.chmod(curFilename, '444', function(err) {
        if (err) {
          console.error('could not change permission on %s to 444', curFilename);
        }
      });
    });
    return ws;
  };
  var factory = function() {
    return lazywritestream.create(wsFactory);
  };
  return factory;
};

function createCheckRotate(tableName, basePath) {
  var check = function () {
    var filename = datafileName(tableName, basePath);
    return filename !== curFilenames[tableName];
  };
  return check;
};

function datafileName(tableName, basePath) {
  return basePath + '/' + tableName + '.' + new Date().getTime() + '.data';
};
