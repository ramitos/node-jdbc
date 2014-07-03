var _ = require('underscore');
var java = require('java');

function trim1 (str) {
  return (str || '').replace(/^\s\s*/, '').replace(/\s\s*$/, '');
}

function JDBCConn() {
  if (!(this instanceof JDBCConn)) return new JDBCConn();
  this._config = {};
  this._conn = null;
  this._drivernames = [];
}

JDBCConn.prototype.initialize = function(config, callback) {
  var self = this;
  self._config = config;

  if(java.classpath.indexOf(self._config.libpath) < 0){
    java.classpath.push(self._config.libpath);
  }

  if(self._drivernames.indexOf(self._config.drivername) >= 0){
    return callback(null, self._config.drivername);
  }

  java.newInstance(self._config.drivername, function(err, driver) {
    if (err) {
      return callback(err);
    } else {
      java.callStaticMethod('java.sql.DriverManager','registerDriver', driver, function(err, result) {
        if (!err){
          self._drivernames.push(self._config.drivername);
        }

        callback(err, self._config.drivername);
      });
    }
  });
};

JDBCConn.prototype.open = function(callback) {
  var self = this;

  java.callStaticMethod('java.sql.DriverManager','getConnection', self._config.url, function(err, conn) {
    if (err) {
      return callback(err);
    } else {
      self._conn = conn;
      return callback(null, conn);
    }
  });
};

JDBCConn.prototype.close = function(callback) {
  var self = this;

  if (self._conn) {
    self._conn.close(function(err) {
      if (err) {
        return callback(err);
      } else {
        self._conn = null;
        return callback(null);
      }
    });
  }
};

JDBCConn.prototype.executeQuery = function(sql, callback) {
  var self = this;

  self._conn.createStatement(function(err, statement) {
    if (err) {
      return callback(err);
    } else {
      statement.executeQuery(sql ,function(err,resultset) {
        var resultset = resultset;
        if (err) {
          return callback(err);
        } else if (resultset) {
          resultset.getMetaData(function(err,rsmd) {
            if (err) {
              return callback(err);
            } else {
              var cc = rsmd.getColumnCountSync();
              var columns = [''];
              for(var i = 1; i <= cc; i++) {
                var colname = rsmd.getColumnNameSync(i);
                columns.push(colname)
              }
              var results = [];
              var next = resultset.nextSync();

              while(next) {
                var row = {};

                for(var i = 1; i <= cc; i++) {
                  row[columns[i]] = trim1(resultset.getStringSync(i));
                }
                results.push(row);
                next = resultset.nextSync();
              }
              return callback(null, results);
            }
          });
        } else {
          return callback(null, null);
        }
      });
    }
  });
};

JDBCConn.prototype.executeUpdate = function(sql, callback) {
  var self = this;

  self._conn.createStatement(function(err, statement) {
    if (err) {
      return callback(err);
    } else {
      statement.executeUpdate(sql, function(err, rowcount) {
        if (err) {
          return callback(err);
        } else {
          return callback(null, rowcount);
        }
      });
    }
  });
};