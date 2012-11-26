var fs = require('fs');
var stream = require('stream');
var util = require('util');

var LazyFileWriteStream = function(filename, options) {
  this.readable = false;
  this.writeable = true;

  this.filename = filename;
  this.options = options;
  this.stream = null;

  // Set this up so our change permission stream can
  // do its job
  this.path = filename;
};

util.inherits(LazyFileWriteStream, stream);

LazyFileWriteStream.prototype._writeData = function(data) {
  if (!this.stream) {
    console.log('first write, creating stream');
    this.stream = fs.createWriteStream(this.filename, this.options);

    var self = this;
    this.stream.on('drain', function() {
      self.emit('drain');
    });
    this.stream.on('error', function(err) {
      self.emit('error', err);
    });
    this.stream.on('close', function() {
      self.emit('close');
    });
    this.stream.on('pipe', function() {
      self.emit('pipe');
    });
  }
  return this.stream.write(data);
};

LazyFileWriteStream.prototype.write = function() {
  return this._writeData.apply(this, arguments);
};

LazyFileWriteStream.prototype.end = function() {
  if (this.stream) {
    this.stream.end.apply(this.stream, arguments);
  }
};

exports.LazyFileWriteStream = LazyFileWriteStream;
