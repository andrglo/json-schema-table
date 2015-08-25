var pg = require('pg');
var mssql = require('mssql');
var spec = require('./spec');
var gutil = require('gulp-util');
var pretty = require('pretty-hrtime');

var databaseName = 'json-schema-table';

var mssqlConfig = {
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASSWORD,
  server: process.env.MSSQL_HOST,
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

pg.defaults.user = process.env.POSTGRES_USER || 'postgres';
pg.defaults.database = 'postgres';
pg.defaults.password = process.env.POSTGRES_PASSWORD;
pg.defaults.port = process.env.POSTGRES_PORT || 5432;
pg.defaults.host = process.env.POSTGRES_HOST || 'localhost';
pg.defaults.poolSize = 25;
pg.defaults.poolIdleTimeout = 30000;

function createPostgresDb() {
  var dbName = process.env.POSTGRES_DATABASE || databaseName;
  return new Promise(function(resolve, reject) {
    pg.connect(function(err, client, done) {
      if (err) return reject(err);
      client.query('DROP DATABASE IF EXISTS "' + dbName + '";', function(err, result) {
        if (err) {
          done();
          reject(err);
          return;
        }
        client.query('CREATE DATABASE "' + dbName + '"', function(err, result) {
          if (err) {
            done();
            reject(err);
            return;
          }
          done();
          resolve();
        });
      });
    });
  });
}

function createMssqlDb() {
  var dbName = process.env.MSSQL_DATABASE || databaseName;
  return (new mssql.Request()).batch(
    'IF EXISTS(select * from sys.databases where name=\'' +
    dbName + '\') DROP DATABASE [' + dbName + '];' +
    'CREATE DATABASE [' + dbName + '];'
  );
}

before(function(done) {
  if (process.env.CI) {
    return createPostgresDb()
      .then(function() {
        pg.defaults.database = process.env.POSTGRES_DATABASE || databaseName;
        done();
      })
      .catch(function(error) {
        done(error);
      });
  }
  mssql.connect(mssqlConfig)
    .then(function() {
      return createMssqlDb()
        .then(function() {
          return mssql.close();
        })
        .then(function() {
          mssqlConfig.database = process.env.MSSQL_DATABASE || databaseName;
          return mssql.connect(mssqlConfig);
        });
    })
    .then(function() {
      return createPostgresDb();
    })
    .then(function() {
      pg.defaults.database = process.env.POSTGRES_DATABASE || databaseName;
      done();
    })
    .catch(function(error) {
      done(error);
    });
});

describe('postgres', function() {
  var duration;
  before(function() {
    duration = process.hrtime();
  });
  spec(pg);
  after(function() {
    duration = process.hrtime(duration);
    gutil.log('Postgres finished after', gutil.colors.magenta(pretty(duration)));
  });
});

describe('mssql', function() {
  if (process.env.CI) {
    return;
  }
  var duration;
  before(function() {
    duration = process.hrtime();
  });
  spec(mssql);
  after(function() {
    duration = process.hrtime(duration);
    gutil.log('Mssql finished after', gutil.colors.magenta(pretty(duration)));
  });
});

after(function() {
  if (!process.env.CI) {
    mssql.close();
  }
  pg.end();
});
