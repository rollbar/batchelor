exports.readablePermissions = function(statsMode) {
  var s = (statsMode & parseInt ("777", 8)).toString(8);
  while (s.length !== 3) {
    s = "0" + s;
  }
  return s;
};
