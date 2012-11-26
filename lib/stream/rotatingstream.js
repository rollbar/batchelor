var stream = require('stream');
var util = require('util');

/*
 * RotatingStream
 *
 * Writes data to a stream and rotates onto a new one
 * based on configurable rules.
 *
 * Events:
 *
 *  'data' - emitted when the destination stream is changed.
 *    Passes along the following arguments to emit:
 *      previousStream - the previous stream that was
 *                       written to.
 *      newStream - the stream that is currently being written to.
 *      prevBytes - the number of bytes written to the previous stream.
 *
 *  'drain' - emitted when it is safe to continue writing to this stream
 *    which will be the case when the current stream emits a 'drain'
 *    event or when the current stream is rotated.
 */

var RotatingStream = function(streamFactory, checkRotateFn, checkRotateInterval, options) {
  options = options || {};

  this.readable = true;
  this.writable = true;

  this.streamFactory = streamFactory;
  this.checkRotate = checkRotateFn;
  this.checkRotateInterval = checkRotateInterval || 1000;
  console.log(checkRotateFn);

  this.encoding = options.encoding || 'utf-8';

  this.bytesWritten = 0;
  this.totalBytesWritten = 0;

  // Create the initial stream to write to
  this._rotateCurrentStream();

  // Create a timer which will check to see if we should rotate
  // on the given interval. This is just a timer to check, depending
  // on the value of self.checkRotate() so the stream may or may not be
  // rotated.
  var self = this;
  var resetRotateTimer = function() {
    self.rotateTimer = setTimeout(function() {
      console.log('rotate timer activated');
      if (self.checkRotate()) {
        self._rotateCurrentStream();
      }
      resetRotateTimer();
    }, self.checkRotateInterval);
  };
  resetRotateTimer();
};

util.inherits(RotatingStream, stream);

RotatingStream.prototype._rotateCurrentStream = function(doNotContinue) {
  var prevStream = this.currentStream;
  var prevBytesWritten = this.bytesWritten;

  if (this.currentStream) {
    console.log('ending current stream');
    this.currentStream.end();
  }

  if (doNotContinue) {
    console.log('rotating stream finishing');
    // one last data event if there was previous bytes written
    if (prevBytesWritten) {
      this.emit('data', prevStream, null, prevBytesWritten);
    }
    return;
  }

  this.currentStream = this.streamFactory();

  var self = this;
  var curStream = this.currentStream;
  curStream.on('drain', function() {
    // Only emit the drain event if the current stream
    // matches this stream since the rotate event will
    // happen alongside the drain event.
    if (self.currentStream === curStream) {
      self.emit('drain');
    }
  });
  curStream.on('error', function(err) {
    console.error(err);
    curStream.destroy();
    self.emit('error', err);
  });

  this.bytesWritten = 0;

  if (prevStream) {
    // Only emit the data event when the previous stream was actually written to.
    if (prevBytesWritten) {
      this.emit('data', prevStream, this.currentStream, prevBytesWritten);
    }
    this.emit('drain');
  }
};

RotatingStream.prototype._writeData = function(data) {
  if (!Buffer.isBuffer(data)) {
    data = new Buffer(data, this.encoding);
    var size = data.length;
    this.bytesWritten += size;
    this.totalBytesWritten += size;
  }
  return this.currentStream.write(data);
};

RotatingStream.prototype.write = function() {
  console.log('rotate stream write');
  this._writeData.apply(this, arguments);
};

RotatingStream.prototype.end = function() {
  console.log('rotate stream end');
  if (arguments.length) {
    this._writeData.apply(this, arguments);
  }

  console.log('clearing timeout:', this.rotateTimer);
  clearTimeout(this.rotateTimer);
  this._rotateCurrentStream(true);
  this.emit('end');
};

RotatingStream.prototype.changeRotationInterval = function(newInterval) {
  this.checkRotateInterval = newInterval;
};

exports.RotatingStream = RotatingStream;
