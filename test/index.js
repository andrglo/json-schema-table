const spec = require('./spec')
const PgCrLayer = require('pg-cr-layer')
const MssqlCrLayer = require('mssql-cr-layer')

const pgConfig = {
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
  database: 'postgres',
  host: process.env.POSTGRES_HOST || 'localhost',
  port: process.env.POSTGRES_PORT || 5432,
  pool: {
    max: 10,
    idleTimeout: 30000
  }
}
const pg = new PgCrLayer(pgConfig)

const mssqlConfig = {
  user: process.env.MSSQL_USER || 'sa',
  password: process.env.MSSQL_PASSWORD || 'Passw0rd',
  database: 'master',
  host: process.env.MSSQL_HOST || 'localhost',
  port: process.env.MSSQL_PORT || 1433,
  pool: {
    max: 10,
    idleTimeout: 30000
  }
}
const mssql = new MssqlCrLayer(mssqlConfig)

const databaseName = process.env.POSTGRES_DATABASE || 'test-json-schema-table'

function createPostgresDb() {
  return pg
      .execute('DROP DATABASE IF EXISTS "' + databaseName + '";')
      .then(function() {
        return pg.execute('CREATE DATABASE "' + databaseName + '"')
      })
      .then(function() {
        return pg.execute('DROP DATABASE IF EXISTS "' + databaseName + '2";')
      })
      .then(function() {
        return pg.execute('CREATE DATABASE "' + databaseName + '2"')
      })
}

function createMssqlDb() {
  return mssql.execute(
      'IF EXISTS(select * from sys.databases where name=\'' +
      databaseName +
      '\') DROP DATABASE [' +
      databaseName +
      '];' +
      'CREATE DATABASE [' +
      databaseName +
      '];' +
      'IF EXISTS(select * from sys.databases where name=\'' +
      databaseName +
      '2\') DROP DATABASE [' +
      databaseName +
      '2];' +
      'CREATE DATABASE [' +
      databaseName +
      '2];'
  )
}

const pgOptions = {database: databaseName}
const mssqlOptions = {database: databaseName}

before(function() {
  return pg
      .connect()
      .then(function() {
        return createPostgresDb()
            .then(function() {
              console.log('Postgres db created')
              return pg.close()
            })
            .then(function() {
              console.log('Postgres db creation connection closed')
              pgConfig.database = databaseName
              console.log('Postgres will connect to', pgConfig.database)
              pgOptions.db = new PgCrLayer(pgConfig)
            })
      })
      .then(function() {
        if (!process.env.CI) {
          return mssql.connect().then(function() {
            return createMssqlDb()
                .then(function() {
                  console.log('Mssql db created')
                  return mssql.close()
                })
                .then(function() {
                  console.log('Mssql db creation connection closed')
                  mssqlConfig.database = databaseName
                  console.log('Mssql will connect to', mssqlConfig.database)
                  mssqlOptions.db = new MssqlCrLayer(mssqlConfig)
                })
          })
        }
      })
})

describe('postgres', function() {
  let duration
  before(function() {
    duration = process.hrtime()
  })
  spec(pgOptions)
  after(function() {
    duration = process.hrtime(duration)
    console.info(
        'postgres finished after: %ds %dms',
        duration[0],
        duration[1] / 1000000
    )
  })
})

describe('mssql', function() {
  let duration
  before(function() {
    duration = process.hrtime()
  })
  spec(mssqlOptions)
  after(function() {
    duration = process.hrtime(duration)
    console.info(
        'mssql finished after: %ds %dms',
        duration[0],
        duration[1] / 1000000
    )
  })
})

after(function() {
  mssqlOptions.db.close()
  pgOptions.db.close()
})
