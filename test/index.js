var spec = require('./spec');
var gutil = require('gulp-util');
var pretty = require('pretty-hrtime');
var PgCrLayer = require('pg-cr-layer');
var MssqlCrLayer = require('mssql-cr-layer');

var pgConfig = {
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD,
  database: 'postgres',
  host: process.env.POSTGRES_HOST || 'localhost',
  port: process.env.POSTGRES_PORT || 5432,
  pool: {
    max: 10,
    idleTimeout: 30000
  }
};
var pg = new PgCrLayer(pgConfig);

var mssqlConfig = {
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASSWORD,
  database: 'master',
  host: process.env.MSSQL_HOST || 'localhost',
  port: process.env.MSSQL_PORT || 1433,
  pool: {
    max: 10,
    idleTimeout: 30000
  }
};
var mssql = new MssqlCrLayer(mssqlConfig);

var databaseName = process.env.POSTGRES_DATABASE || 'test-json-schema-table';

function createPostgresDb() {
  return pg.execute(
    'DROP DATABASE IF EXISTS "' + databaseName + '";')
    .then(function() {
      return pg.execute('CREATE DATABASE "' + databaseName + '"');
    })
    .then(function() {
      return pg.execute('DROP DATABASE IF EXISTS "' + databaseName + '2";');
    })
    .then(function() {
      return pg.execute('CREATE DATABASE "' + databaseName + '2"');
    });
}

function createMssqlDb() {
  return mssql.execute(
    'IF EXISTS(select * from sys.databases where name=\'' +
    databaseName + '\') DROP DATABASE [' + databaseName + '];' +
    'CREATE DATABASE [' + databaseName + '];' +
    'IF EXISTS(select * from sys.databases where name=\'' +
    databaseName + '2\') DROP DATABASE [' + databaseName + '2];' +
    'CREATE DATABASE [' + databaseName + '2];'
  );
}

var pgOptions = {database: databaseName};
var mssqlOptions = {database: databaseName};

before(function(done) {
  return pg.connect()
    .then(function() {
      return createPostgresDb()
        .then(function() {
          gutil.log('Postgres db created');
          return pg.close();
        })
        .then(function() {
          gutil.log('Postgres db creation connection closed');
          pgConfig.database = databaseName;
          gutil.log('Postgres will connect to', pgConfig.database);
          pgOptions.db = new PgCrLayer(pgConfig);
        });
    })
    .then(function() {
      if (!process.env.CI) {
        return mssql.connect()
          .then(function() {
            return createMssqlDb()
              .then(function() {
                gutil.log('Mssql db created');
                return mssql.close();
              })
              .then(function() {
                gutil.log('Mssql db creation connection closed');
                mssqlConfig.database = databaseName;
                gutil.log('Mssql will connect to', mssqlConfig.database);
                mssqlOptions.db = new MssqlCrLayer(mssqlConfig);
              });
          });
      }
    })
    .then(function() {
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
  spec(pgOptions);
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
  spec(mssqlOptions);
  after(function() {
    duration = process.hrtime(duration);
    gutil.log('Mssql finished after', gutil.colors.magenta(pretty(duration)));
  });
});

after(function() {
  if (!process.env.CI) {
    mssqlOptions.db.close();
  }
  pgOptions.db.close();
});
