var pgp = require('pg-promise');
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

var pgConfig = {
  host: process.env.POSTGRES_HOST || 'localhost',
  port: process.env.POSTGRES_PORT || 5432,
  database: 'postgres',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD
};
var pg = pgp();
var pgDb = pg(pgConfig);

function createPostgresDb() {
  var dbName = process.env.POSTGRES_DATABASE || databaseName;
  return pgDb.none('DROP DATABASE IF EXISTS "' + dbName + '";')
    .then(function() {
      return pgDb.none('CREATE DATABASE "' + dbName + '"');
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
        pgConfig.database = process.env.POSTGRES_DATABASE || databaseName;
        pgDb = pg(pgConfig);
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
      pgConfig.database = process.env.POSTGRES_DATABASE || databaseName;
      pgDb = pg(pgConfig);
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
  spec(pgDb);
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
