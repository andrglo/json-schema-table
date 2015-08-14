import pgp from 'pg-promise';
import mssql from 'mssql';
import spec from './spec';

const databaseName = 'json-schema-table';

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
  password: process.env.POSTGRES_PASSWORD || 'postgres'
};
var pg = pgp();
var pgDb = pg(pgConfig);

function createPostgresDb() {
  let dbName = process.env.POSTGRES_DATABASE || databaseName;
  return pgDb.none('DROP DATABASE IF EXISTS "' + dbName + '";')
    .then(function() {
      return pgDb.none('CREATE DATABASE "' + dbName + '"');
    });
}

function createMssqlDb() {
  let dbName = process.env.MSSQL_DATABASE || databaseName;
  return (new mssql.Request()).batch(
    'IF EXISTS(select * from sys.databases where name=\'' +
    dbName + '\') DROP DATABASE [' + dbName + '];' +
    'CREATE DATABASE [' + dbName + ']');
}

before(function(done) {
  mssql.connect(mssqlConfig)
    .then(function() {
      return createMssqlDb();
    })
    .then(function() {
      return createPostgresDb();
    })
    .then(function() {
      // ? reconnect?
      //mssqlConfig.database = process.env.MSSQL_DATABASE || ;
      //pgConfig.database = process.env.POSTGRES_DATABASE || 'json-schema-table';
      done();
    })
    .catch(function(error) {
      done(error);
    });
});

describe('mssql', function() {
  spec(mssql);
});

describe('postgres', function() {
  spec(pgDb);
});

after(function() {
  mssql.close();
  pg.end();
});
