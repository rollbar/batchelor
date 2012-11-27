var stream = require('stream');
var util = require('util');

var RowStream = require('./stream/rowstream').RowStream;
var RotatingStream = require('./stream/rotatingstream').RotatingStream;
var ChangeFilePermissionStream = require('./stream/changefilepermissionstream').ChangeFilePermissionStream;
var LazyFileWriteStream = require('./stream/lazyfilewritestream').LazyFileWriteStream;

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

var Batchelor = function(tableName, options) {
  this.readable = true;
  this.writable = true;
  this.queuedWrites = [];
  this.queueFlag = false;

  options = options || {};

  var basePath = options.path || __dirname;
  var rotateInterval = options.interval === undefined ? 5000 : options.interval;

  var streamFactory = createStreamFactory(tableName, basePath, 'utf-8');
  var checkRotate = createCheckRotate(tableName, basePath);

  this.rowStream = new RowStream(tableName);
  this.rotatingStream = new RotatingStream(streamFactory,
      checkRotate, rotateInterval / 2, {encoding: 'utf-8'});
  this.changePermStream = new ChangeFilePermissionStream('444');

  // Hook up the write events from this.rowStream and the read
  // events from this.changePermStream
  var self = this;
  this.changePermStream.on('data', function(filename) {
    self.emit('data', filename);
  });

  this.rowStream.on('error', function(err) {
    self.emit('error', err);
  });

  this.rowStream.on('end', function() {
    self.emit('end');
  });

  // Hook up the streams so that when we write to the RowStream it will
  // get written to the proper file and rotated into a read-only file.
  this.rowStream.pipe(this.rotatingStream).pipe(this.changePermStream);
};

util.inherits(Batchelor, stream);

Batchelor.prototype._writeData = function(data) {
  var queuedWrites = this.queuedWrites;
  if (this.queueFlag) {
    console.log('wrote to queue in position %d', this.queuedWrites.length);
    queuedWrites.push(data);
  } else {
    var rowStream = this.rowStream;

    // queue up writes until we can drain out the rowStream
    if (!rowStream.write(data)) {
      console.log('kernel buffer is full, queuing subsequent writes');
      this.queueFlag = true; 

      var self = this;
      var onDrain = function() {
        console.log('onDrain called');
        var cur;

        // XXX(cory): remove me
        var tmp = queuedWrites.length;

        while (queuedWrites.length) {
          cur = queuedWrites.splice(0, 1);
          if (!rowStream.write(cur)) {
            console.log('kernel buffer is full again... queuing writes');
            rowStream.once('drain', onDrain);
            return;
          }
        }
        console.log('drained %d writes', tmp);

        // If we've gotten here then there are no more queued
        // writes so we can stop queuing and move on.
        self.queueFlag = false;
      };
      rowStream.once('drain', onDrain);
    }
  }
  // Since we're handling the queuing ourselves we don't
  // need to expose the drain event to the user and we can
  // always return true. This is a bit dangerous because it's
  // possible that the user is writing faster than we can drain
  // the internal buffer but... they'll have to deal for now.
  return true;
};

Batchelor.prototype.write = function() {
  return this._writeData.apply(this, arguments);
};

Batchelor.prototype.end = function() {
  if (arguments.length) {
    this._writeData.apply(this, arguments);
  }
  this.rowStream.end();
};

Batchelor.prototype.destroy = function() {
  return this.rowStream.destroy();
};

exports.Batchelor = Batchelor;
exports.RowStream = RowStream;
exports.RotatingStream = RotatingStream;
exports.ChangeFilePermissionStream = ChangeFilePermissionStream;
exports.LazyFileWriteStream = LazyFileWriteStream;

/**** Internal ****/

var curFilenames = {};
function createStreamFactory(tableName, basePath, encoding) {
  var factory = function() {
    var curFilename = datafileName(tableName, basePath);
    curFilenames[tableName] = curFilename;
    return new LazyFileWriteStream(curFilename, {flags: 'a', encoding: encoding});
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
