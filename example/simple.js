var batchelor = require('../lib/batchelor');

/*
 * A simple example that writes two audit_event rows to two
 * separate files in /tmp/ and then shuts down.
 */

var auditEvent = new batchelor.Batchelor('audit_event', {path: '/tmp', interval: 3000});
auditEvent.write({timestamp: 12345, eventName: 'pageview', id: 54321});

setTimeout(function() {
  auditEvent.write({timestamp: 22222, eventName: 'poop', id: 54321});
  auditEvent.end();
}, 10000);
