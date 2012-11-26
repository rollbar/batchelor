var fs = require('fs');
var stream = require('stream');
var util = require('util');

/*
 * ChangeFilePermissionStream
 *
 */

var ChangeFilePermissionStream = function(mode) {
  this.readable = true;
  this.writable = true;
  this.mode = mode;
};

util.inherits(ChangeFilePermissionStream, stream);

ChangeFilePermissionStream.prototype._writeData = function(data) {
  var dataType = typeof data;
  if ((data instanceof stream) && data.path) {
    data = data.path;
  } else if (dataType !== 'string') {
    console.log(dataType, data);
    this.emit('error', new Error('expected a string got ' + dataType));
    return true;
  }
  var mode = this.mode;
  var self = this;

  console.log('changing permission of %s to %s', data, mode);
  fs.chmod(data, mode, function(err) {
    if (err) {
      console.error('could not change permission on %s to %s', data, mode);
      self.emit('error', err);
    } else {
      self.emit('data', data);
    }
  });
  return true;
};

ChangeFilePermissionStream.prototype.write = function() {
  console.log('change perm write');
  return this._writeData.apply(this, arguments);
};

ChangeFilePermissionStream.prototype.end = function() {
  console.log('change perm end');
  if (arguments.length) {
    this._writeData.apply(this, arguments);
  }
  this.emit('end');
};

exports.ChangeFilePermissionStream = ChangeFilePermissionStream;
