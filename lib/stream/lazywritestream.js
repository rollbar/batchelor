var stream = require('stream');

// creates a new write stream on first write() call
exports.create = function(wsFactory) {
  var ws;
  var s = new stream();
  s.readable = false;
  s.writable = true;

  s.write = function(buf) {
    if (!ws) {
      ws = wsFactory();

      var self = this;
      ws.on('drain', function() { self.emit('drain'); });
      ws.on('error', function(err) { self.emit('error', err); });
      ws.on('close', function() { self.emit('close'); });
      ws.on('pipe', function(src) { self.emit('pipe', src); });
    }
    return ws.write(buf);
  };

  s.end = function(buf) {
    if (arguments.length) {
      s.write(buf);
    }
    s.writable = false;
    if (ws) {
      ws.end();
    }
  };

  s.destroy = function () {
    s.writable = false;
    if (ws) {
      ws.destroy();
    }
  };

  return s;
};
