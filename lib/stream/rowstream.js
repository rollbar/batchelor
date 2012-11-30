var eventstream = require('event-stream');

exports.create = function(tableName) {
  return eventstream.through(
      function write(data) {
        var dataType = (Array.isArray(data) ? 'array' : data === null ? 'null' : typeof data);
        if (dataType !== 'object') {
          this.emit('error', new Error('expected object, got ' + dataType));
        } else {
          try {
            this.emit('data', JSON.stringify({table: tableName, row: data}) + '\n');
          } catch (e) {
            this.emit('error', e);
          }
        }
      },
      function end() {
        this.emit('end');
      });
};
