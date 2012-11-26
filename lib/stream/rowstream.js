var stream = require('stream');
var util = require('util');

/* 
 * RowSteam
 *
 * Converts objects into rows to be written to a batch file.
 *
 */

var RowStream = function(tableName) {
  this.readable = true;
  this.writeable = true;

  this.tableName = tableName;
};

util.inherits(RowStream, stream);

RowStream.prototype._writeRow = function(data) {
  var dataType = typeof data;
  if (!data || dataType !== 'object') {
    this.emit('error', new Error('expected object, got %s', dataType));
  } else {
    var dataObj = {table: this.tableName, row: data}
    var dataStr;
    try {
      dataStr = JSON.stringify(dataObj) + '\n';
    } catch (e) {
      this.emit('error', e);
      return;
    }
    this.emit('data', dataStr);
  }
  return true;
};

RowStream.prototype.write = function() {
  console.log('row stream write');
  return this._writeRow.apply(this, arguments);
};

RowStream.prototype.end = function() {
  console.log('row stream end');
  if (arguments.length) {
    this._writeRow.apply(this, arguments);
  }
  this.emit('end');
};

exports.RowStream = RowStream;
