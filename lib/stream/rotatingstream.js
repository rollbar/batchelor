var eventstream = require('event-stream');
var fs = require('fs');
var stream = require('stream');

// Creates a new readable/writable stream which will write to a file and
// rotate it based on configurable rules. Once it's rotated, a 'data'
// event will be emitted containing the previous stream.
exports.create = function(wsFactory, checkRotate, checkRotateInterval) {
  var writeStream = wsFactory();

  var rotate = function() {
    checkRotate(writeStream, function(shouldRotate) {
      if (shouldRotate) {
        writeStream.end();
        writeStream.removeAllListeners();
        throughStream.emit('data', writeStream);

        writeStream = wsFactory(); 
        writeStream.once('error', function(err) {
          throughStream.emit('error', err);
        });

        // Used for testing
        throughStream.currentStream = writeStream;
      }
      timer = setTimeout(rotate, checkRotateInterval);
    });
  };
  
  var timer = setTimeout(rotate, checkRotateInterval);

  var throughStream = eventstream.through(
      function write(data) {
        writeStream.write(data);
        throughStream.totalBytesWritten += data.length;
      },
      function end() {
        clearTimeout(timer);
        writeStream.end();
        this.emit('end');
      });

  throughStream.totalBytesWritten = 0;
  throughStream.currentStream = writeStream;

  return throughStream;
};
