const _ = require('lodash')
const assert = require('assert')

const utils = require('./utils')

module.exports = jsonSchemaTable

function jsonSchemaTable(tableName, schema, config) {
  config = Object.assign({}, config)
  assert(
      config.db,
      'Database connector not informed, should be one of: ' +
      'mssql-cr-layer or pg-cr-layer'
  )
  const dialect = {
    db: config.db
  }
  if (config.db.dialect === 'mssql') {
    dialect.datetime = config.datetime
    dialect.propertyToDb = propertyToMssql
  } else {
    dialect.propertyToDb = propertyToPostgres
    dialect.bigint = !!config.bigint
    dialect.doubleFloats = !!config.doubleFloats
  }

  const dbSchemaName =
    config.schema || (dialect.db.dialect === 'mssql' ? 'dbo' : 'public')

  return {
    create: function() {
      return Promise.resolve()
          .then(function() {
            return dialect.db.execute(
                createTable(dialect, tableName, dbSchemaName, schema)
            )
          })
          .catch(function(error) {
            wrapError(error, [dbSchemaName, tableName].join('.'))
            throw error
          })
    },
    sync: function() {
      return getDbMetadata(dialect, tableName, dbSchemaName)
          .then(function(metadata) {
            if (!tableExists(metadata)) {
              throw new Error('All tables should be created first')
            }
            return checkTableStructure(
                dialect,
                tableName,
                dbSchemaName,
                schema,
                metadata
            ).then(function() {
              return createTableReferences(
                  dialect,
                  tableName,
                  dbSchemaName,
                  schema,
                  metadata
              )
            })
          })
          .catch(function(error) {
            wrapError(error, tableName)
            throw error
          })
    },
    metadata: function() {
      return getDbMetadata(dialect, tableName, dbSchemaName)
          .then(function(metadata) {
            const tableMetadata = {columns: metadata.columns}
            const primaryKey = metadata.tablesWithPrimaryKey[tableName]
            if (primaryKey) {
              tableMetadata.primaryKey = primaryKey.map(function(column) {
                return column.name
              })
            }
            const uniqueKeys = metadata.tablesWithUniqueKeys[tableName]
            if (uniqueKeys) {
              tableMetadata.uniqueKeys = []
              _.forEach(uniqueKeys, function(uniqueKey) {
                tableMetadata.uniqueKeys.push(
                    uniqueKey.map(function(column) {
                      return column.name
                    })
                )
              })
            }
            const foreignKeys = metadata.tablesWithForeignKeys[tableName]
            if (foreignKeys) {
              tableMetadata.foreignKeys = foreignKeys.map(function(foreignKey) {
                return {
                  table: foreignKey.table,
                  columns: foreignKey.columns
                }
              })
            }
            return tableMetadata
          })
          .catch(function(error) {
            wrapError(error, tableName)
            throw error
          })
    }
  }
}

function getDbMetadata(dialect, tableName, dbSchemaName) {
  function isRedshift() {
    return new Promise(function(resolve, reject) {
      if (dialect.db.dialect !== 'postgres') {
        resolve(false)
      } else {
        dialect.db
            .isRedshift()
            .then(function(res) {
              resolve(res)
            })
            .catch(function(err) {
              reject(err)
            })
      }
    })
  }

  const dbToProperty =
    dialect.db.dialect === 'mssql' ? mssqlToProperty : postgresToProperty
  const metadata = {
    tablesWithPrimaryKey: {},
    tablesWithUniqueKeys: {},
    tablesWithForeignKeys: {},
    columns: {}
  }
  const catalog =
    dialect.db.dialect === 'mssql' ? 'db_name()' : 'current_database()'
  return isRedshift()
      .then(function(rs) {
        if (!rs) {
          return dialect.db
              .query(
                  'SELECT pk.CONSTRAINT_NAME as constraint_name,pk.TABLE_NAME as table_name,' +
              'pk.COLUMN_NAME as column_name,' +
              'rfk.TABLE_NAME as ref_table_name,rfk.COLUMN_NAME as ref_column_name,' +
              'c.DATA_TYPE as data_type,c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,' +
              'c.NUMERIC_PRECISION as numeric_precision,c.NUMERIC_SCALE as numerico_scale ' +
              'FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE as pk ' +
              'INNER JOIN INFORMATION_SCHEMA.COLUMNS as c ON pk.COLUMN_NAME=c.COLUMN_NAME AND pk.TABLE_NAME=c.TABLE_NAME AND ' +
              'pk.TABLE_CATALOG=c.TABLE_CATALOG AND pk.TABLE_SCHEMA=c.TABLE_SCHEMA ' +
              'LEFT OUTER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS as rk ON pk.CONSTRAINT_NAME=rk.CONSTRAINT_NAME AND ' +
              'pk.TABLE_CATALOG=rk.CONSTRAINT_CATALOG AND pk.TABLE_SCHEMA=rk.CONSTRAINT_SCHEMA ' +
              'LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as rfk ON rk.UNIQUE_CONSTRAINT_NAME=rfk.CONSTRAINT_NAME ' +
              'AND pk.ORDINAL_POSITION=rfk.ORDINAL_POSITION AND ' +
              'pk.TABLE_CATALOG=rfk.TABLE_CATALOG AND pk.TABLE_SCHEMA=rfk.TABLE_SCHEMA ' +
              'WHERE pk.TABLE_CATALOG=' +
              catalog +
              'AND pk.TABLE_SCHEMA=\'' +
              dbSchemaName +
              '\'' +
              'ORDER BY pk.TABLE_NAME,pk.CONSTRAINT_NAME,pk.ORDINAL_POSITION'
              )
              .then(function(recordset) {
                const constraints = {}
                recordset.map(function(record) {
                  if (!constraints[record.constraint_name]) {
                    constraints[record.constraint_name] = {
                      table: record.table_name,
                      references: record.ref_table_name,
                      columns: []
                    }
                  }

                  const constraint = constraints[record.constraint_name]
                  constraint.columns.push(dbToProperty(record))
                })
                return constraints
              })
        } else {
        // Redshift does not support PK or FK constraints so just return an empty array.
          return Promise.resolve({})
        }
      })
      .then(function(constraints) {
        _.forEach(constraints, function(constraint, constraintName) {
          const table = constraint.table
          const constraintType = constraintName.substr(0, 2).toLowerCase()
          switch (constraintType) {
            case 'pk':
              metadata.tablesWithPrimaryKey[table] = constraint.columns
              break
            case 'fk':
              const fk = (metadata.tablesWithForeignKeys[table] =
              metadata.tablesWithForeignKeys[table] || [])
              fk.push({
                table: constraint.references,
                columns: constraint.columns
              })
              break
            case 'uk':
              const uk = (metadata.tablesWithUniqueKeys[table] =
              metadata.tablesWithUniqueKeys[table] || [])
              uk.push(constraint.columns)
              break
          }
        })
        return dialect.db.query(
            'SELECT COLUMN_NAME as column_name,IS_NULLABLE as is_nullable,' +
          'DATA_TYPE as data_type,' +
          'CHARACTER_MAXIMUM_LENGTH as character_maximum_length,NUMERIC_PRECISION as numeric_precision,' +
          'NUMERIC_SCALE as numeric_scale FROM ' +
          'INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=\'' +
          tableName +
          '\'' +
          'AND TABLE_CATALOG=' +
          catalog +
          'AND TABLE_SCHEMA=\'' +
          dbSchemaName +
          '\''
        )
      })
      .then(function(recordset) {
        recordset.map(function(record) {
          metadata.columns[record.column_name] = dbToProperty(record)
        })
        return metadata
      })
}

function createTable(dialect, tableName, dbSchemaName, schema) {
  const columns = []
  const primaryKey = utils.mapToColumnName(schema.primaryKey, schema) || []
  const primaryKeyDefined = primaryKey.length > 0
  const unique = []

  const qualifiedTableName = [
    dialect.db.wrap(dbSchemaName),
    dialect.db.wrap(tableName)
  ].join('.')

  _.forEach(schema.properties, function(property, name) {
    const fieldName = property.field || name
    const fieldType = dialect.propertyToDb(property, name, schema)
    if (fieldType === void 0) {
      return
    }
    columns.push(dialect.db.wrap(fieldName) + ' ' + fieldType)
    if (primaryKeyDefined === false && property.primaryKey === true) {
      primaryKey.push(fieldName)
    }
    if (property.unique === true) {
      unique.push([fieldName])
    }
  })
  if (primaryKey.length) {
    columns.push(
        'CONSTRAINT ' +
        dialect.db.wrap(buildPkConstraintName(tableName, primaryKey)) +
        ' PRIMARY KEY (' +
        primaryKey
            .map(function(column) {
              return dialect.db.wrap(column)
            })
            .join(',') +
        ')'
    )
  }

  if (schema.unique) {
    schema.unique.map(function(key) {
      unique.push(utils.mapToColumnName(key, schema))
    })
  }
  if (unique.length) {
    unique.map(function(group) {
      columns.push(
          'CONSTRAINT ' +
          dialect.db.wrap(buildUniqueConstraintName(tableName, group)) +
          ' UNIQUE (' +
          group
              .map(function(column) {
                return dialect.db.wrap(column)
              })
              .join(',') +
          ')'
      )
    })
  }

  return dialect.db.dialect === 'mssql'
    ? 'IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE ' +
        `TABLE_SCHEMA = '${dbSchemaName}' AND  TABLE_NAME = '${tableName}')) ` +
        `CREATE TABLE ${qualifiedTableName} (${columns.join(',')})`
    : `CREATE TABLE IF NOT EXISTS ${qualifiedTableName} (${columns.join(',')})`
}

function alterTable(dialect, tableName, dbSchemaName, schema, metadata) {
  const commands = []
  const primaryKey = []
  const unique = []

  const qualifiedTableName = [
    dialect.db.wrap(dbSchemaName),
    dialect.db.wrap(tableName)
  ].join('.')

  _.forEach(schema.properties, function(property, name) {
    const fieldName = property.field || name
    const fieldType = dialect.propertyToDb(property, name, schema, true)
    if (fieldType === void 0) {
      return
    }
    if (
      property.primaryKey === true ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1)
    ) {
      primaryKey.push(fieldName)
    }
    if (property.unique === true) {
      unique.push([fieldName])
    }

    if (!metadata.columns[fieldName]) {
      commands.push(
          'ALTER TABLE ' +
          qualifiedTableName +
          ' ADD ' +
          dialect.db.wrap(fieldName) +
          ' ' +
          dialect.propertyToDb(property, name, schema)
      )
    } else if (!equalDefinitions(metadata.columns[fieldName], property)) {
      if (canAlterColumn(metadata.columns[fieldName], property)) {
        commands.push(
            'ALTER TABLE ' +
            qualifiedTableName +
            ' ALTER COLUMN ' +
            dialect.db.wrap(fieldName) +
            ' ' +
            fieldType
        )
        if (dialect.name === 'postgres') {
          commands.push(
              'ALTER TABLE ' +
              qualifiedTableName +
              ' ALTER COLUMN ' +
              dialect.db.wrap(fieldName) +
              ' ' +
              postgresSetNull(property, name, schema)
          )
        }
      } else {
        throw new Error('Column ' + fieldName + ' cannot be modified')
      }
    }
  })
  const oldPrimaryKey = metadata.tablesWithPrimaryKey[tableName]
    ? metadata.tablesWithPrimaryKey[tableName].map(function(column) {
      return column.name
    })
    : []
  if (_.difference(primaryKey, oldPrimaryKey).length) {
    if (oldPrimaryKey.length) {
      commands.push(
          'ALTER TABLE ' +
          qualifiedTableName +
          ' DROP CONSTRAINT ' +
          dialect.db.wrap(buildPkConstraintName(tableName, oldPrimaryKey))
      )
    }
    if (primaryKey.length) {
      commands.push(
          'ALTER TABLE ' +
          qualifiedTableName +
          ' ADD CONSTRAINT ' +
          dialect.db.wrap(buildPkConstraintName(tableName, primaryKey)) +
          ' PRIMARY KEY (' +
          primaryKey
              .map(function(column) {
                return dialect.db.wrap(column)
              })
              .join(',') +
          ')'
      )
    }
  }

  if (schema.unique) {
    schema.unique.map(function(key) {
      unique.push(utils.mapToColumnName(key, schema))
    })
  }
  if (unique.length) {
    unique.map(function(key) {
      if (!uniqueKeyExists(key, metadata.tablesWithUniqueKeys[tableName])) {
        commands.push(
            'ALTER TABLE ' +
            qualifiedTableName +
            ' ADD CONSTRAINT ' +
            dialect.db.wrap(buildUniqueConstraintName(tableName, key)) +
            ' UNIQUE (' +
            key
                .map(function(column) {
                  return dialect.db.wrap(column)
                })
                .join(',') +
            ')'
        )
      }
    })
  }

  return commands.join(';')
}

function createTableReferences(
    dialect,
    tableName,
    dbSchemaName,
    schema,
    metadata
) {
  const commands = []

  const qualifiedTableName = [
    dialect.db.wrap(dbSchemaName),
    dialect.db.wrap(tableName)
  ].join('.')

  const $refs = []
  _.forEach(schema.properties, function(property, name) {
    const $ref = property.$ref || (property.schema && property.schema.$ref)
    if ($ref) {
      const referencedTableName = getReferencedTableName($ref)
      if (
        metadata.tablesWithPrimaryKey[referencedTableName] ||
        metadata.tablesWithUniqueKeys[referencedTableName]
      ) {
        const foreignKey = property.field || name
        let key = property.schema && property.schema.key
        if (!key) {
          const pk = metadata.tablesWithPrimaryKey[referencedTableName]
          if (pk && pk.length === 1) {
            key = pk[0].name
          }
        }
        assert(
            key,
            'Foreign key "' +
            foreignKey +
            '" don\'t have a candidate key column defined in table "' +
            referencedTableName +
            '"'
        )
        $refs.push({
          table: referencedTableName,
          key: [key],
          foreignKey: [foreignKey]
        })
      }
    }
  })
  _.forEach(schema.foreignKeys, function(keys, referencedTableName) {
    const fk = {
      table: referencedTableName,
      key: [],
      foreignKey: []
    }
    _.forEach(keys, function(foreignKey, key) {
      fk.key.push(key)
      fk.foreignKey.push(foreignKey)
    })
    $refs.push(fk)
  })

  _.forEach($refs, function($ref) {
    const table = $ref.table

    const primaryKey = metadata.tablesWithPrimaryKey[table]
    const candidateKeys = primaryKey ? [primaryKey] : []
    _.forEach(metadata.tablesWithUniqueKeys[table], function(key) {
      candidateKeys.push(key)
    })
    let candidateKey
    _.forEach(candidateKeys, function(ck) {
      if (
        ck
            .map(function(k) {
              return k.name
            })
            .join() === $ref.key.join()
      ) {
        candidateKey = ck
        return false
      }
    })
    if (!candidateKey) {
      throw new Error(
          'Table "' +
          table +
          '" has no candidate key for "' +
          $ref.key.join(',') +
          '" to be referenced'
      )
    }

    let hfk = false
    const hash = (
      $ref.foreignKey.join('') +
      table +
      $ref.key.join('')
    ).toLowerCase()
    _.forEach(metadata.tablesWithForeignKeys[tableName], function(fk) {
      const fkHash = (
        fk.columns.reduce(function(columns, column) {
          return columns + column.name
        }, '') +
        fk.table +
        fk.columns.reduce(function(columns, column) {
          return columns + column.references
        }, '')
      ).toLowerCase()
      if (hash === fkHash) {
        hfk = true
        return false
      }
    })

    if (hfk === false) {
      $ref.foreignKey.map(function(foreignKey, index) {
        if (!metadata.columns[foreignKey]) {
          const property = candidateKey.reduce(function(result, key) {
            return result || (key.name === $ref.key[index] && key)
          }, void 0)
          commands.push(
              'ALTER TABLE ' +
              qualifiedTableName +
              ' ADD ' +
              dialect.db.wrap(foreignKey) +
              ' ' +
              dialect.propertyToDb(property, foreignKey)
          )
        }
      })

      const constraintName =
        'FK__' + tableName + '__' + $ref.foreignKey.join('__')
      commands.push(
          'ALTER TABLE ' +
          qualifiedTableName +
          ' ADD CONSTRAINT ' +
          dialect.db.wrap(constraintName) +
          ' FOREIGN KEY (' +
          $ref.foreignKey
              .map(function(column) {
                return dialect.db.wrap(column)
              })
              .join(',') +
          ') REFERENCES ' +
          dialect.db.wrap(table) +
          ' (' +
          $ref.key
              .map(function(column) {
                return dialect.db.wrap(column)
              })
              .join(',') +
          ')'
      )
    }
  })

  return commands.length
    ? dialect.db.execute(commands.join(';'))
    : Promise.resolve()
}

function propertyToMssql(property, name, schema) {
  let column
  switch (property.type) {
    case 'integer':
      column = 'INT'
      if (property.autoIncrement === true) {
        column += ' IDENTITY(1,1)'
      }
      break
    case 'text':
      column = 'NVARCHAR(MAX)'
      break
    case 'blob':
      column = 'VARBINARY(MAX)'
      break
    case 'string':
      column =
        'NVARCHAR(' + (property.maxLength ? property.maxLength : 'MAX') + ')'
      break
    case 'date':
      column = 'DATE'
      break
    case 'datetime':
      if (property.timezone === true) {
        column = 'DATETIMEOFFSET'
      } else {
        column = 'DATETIME2'
      }
      break
    case 'number':
      if (property.decimals) {
        column = 'DEC(' + property.maxLength + ',' + property.decimals + ')'
      } else {
        column = 'INT'
      }
      break
    case 'object':
    case void 0:
      return void 0
    case 'array':
      throw new Error(
          'Property ' + name + ' has type not yet implemented: ' + property.type
      )
    default:
      throw new Error('Property ' + name + ' has no correspondent mssql type')
  }
  column +=
    ' ' +
    (property.required === true ||
    property.primaryKey === true ||
    (schema && utils.isInArray(name, schema.primaryKey, schema)) ||
    (schema && utils.isInArray(name, schema.required, schema))
      ? 'NOT NULL'
      : 'NULL')
  return column
}

function mssqlToProperty(metadata) {
  const property = {name: metadata.column_name}
  switch (metadata.data_type) {
    case 'int':
      property.type = 'integer'
      break
    case 'nvarchar':
      if (metadata.character_maximum_length === -1) {
        property.type = 'text'
      } else {
        property.type = 'string'
        property.maxLength = metadata.character_maximum_length
      }
      break
    case 'varbinary':
      property.type = 'blob'
      break
    case 'date':
      property.type = 'date'
      break
    case 'datetime':
    case 'datetime2':
    case 'datetimeoffset':
      property.type = 'datetime'
      break
    case 'decimal':
      property.type = 'number'
      property.maxLength = metadata.numeric_precision
      property.decimals = metadata.numeric_scale
      break
    default:
      throw new Error(
          'Mssql column ' +
          metadata.column_name +
          ' type ' +
          metadata.data_type +
          ' has no correspondent property type'
      )
  }
  if (metadata.is_nullable === 'NO') {
    property.required = true
  }
  if (metadata.ref_column_name) {
    property.references = metadata.ref_column_name
  }
  return property
}

function propertyToPostgres(property, name, schema, isAlter) {
  let column
  const integerType = this.bigint ? 'BIGINT' : 'INTEGER'
  const floatType = this.doubleFloats ? 'DOUBLE PRECISION' : 'REAL'

  switch (property.type) {
    case 'integer':
      if (property.autoIncrement === true) {
        column = 'SERIAL'
      } else {
        column = integerType
      }
      break
    case 'text':
      column = 'TEXT'
      break
    case 'boolean':
      column = 'BOOLEAN'
      break
    case 'blob':
      column = 'BYTEA'
      break
    case 'string':
      if (property.format === 'date-time') {
        column = 'TIMESTAMPTZ'
      } else {
        if (property.maxLength) {
          column = 'VARCHAR(' + property.maxLength + ')'
        } else {
          column = 'TEXT'
        }
      }
      break
    case 'date':
      column = 'DATE'
      break
    case 'time':
      column = 'TIME WITH TIME ZONE'
      break
    case 'datetime':
      column = 'TIMESTAMP WITH TIME ZONE'
      break
    case 'number':
      if (property.decimals && property.decimals > 0) {
        column = 'NUMERIC(' + property.maxLength + ',' + property.decimals + ')'
      } else if (property.maxLength > 0) {
        column = integerType
      } else {
        column = floatType
      }
      break
    case 'object':
    case void 0:
      return void 0
    case 'array':
      throw new Error(
          'Property ' + name + ' has type not yet implemented: ' + property.type
      )
    default:
      throw new Error(
          'Property ' + name + ' has no correspondent postgres type'
      )
  }
  if (isAlter === true) {
    column = ' TYPE ' + column
  } else {
    column +=
      ' ' +
      (property.required === true ||
      property.primaryKey === true ||
      (schema && utils.isInArray(name, schema.primaryKey, schema)) ||
      (schema && utils.isInArray(name, schema.required, schema))
        ? 'NOT NULL'
        : 'NULL')
  }
  return column
}

function postgresSetNull(property, name, schema) {
  return property.required === true ||
    property.primaryKey === true ||
    (schema && utils.isInArray(name, schema.primaryKey, schema)) ||
    (schema && utils.isInArray(name, schema.required, schema))
    ? 'SET NOT NULL'
    : 'DROP NOT NULL'
}

function postgresToProperty(metadata) {
  const property = {name: metadata.column_name}
  switch (metadata.data_type) {
    case 'integer':
    case 'boolean':
    case 'text':
    case 'date':
      property.type = metadata.data_type
      break
    case 'bigint':
      property.type = 'integer'
      break
    case 'real':
    case 'double precision':
      property.type = 'number'
      break
    case 'time':
      property.type = 'time'
      break
    case 'time with time zone':
      property.type = 'time'
      break
    case 'timestamp':
      property.type = 'datetime'
      break
    case 'timestamp without time zone':
    case 'timestamp with time zone':
      property.type = 'datetime'
      break
    case 'character varying':
      property.type = 'string'
      property.maxLength = metadata.character_maximum_length
      break
    case 'bytea':
      property.type = 'blob'
      break
    case 'numeric':
      property.type = 'number'
      property.maxLength = metadata.numeric_precision
      property.decimals = metadata.numeric_scale
      break
    default:
      throw new Error(
          'Postgres column ' +
          metadata.column_name +
          ' type ' +
          metadata.data_type +
          ' has no correspondent property type'
      )
  }
  if (metadata.is_nullable === 'NO') {
    property.required = true
  }
  if (metadata.ref_column_name) {
    property.references = metadata.ref_column_name
  }
  return property
}

function tableExists(metadata) {
  return !_.isEmpty(metadata.columns)
}

function checkTableStructure(
    dialect,
    tableName,
    dbSchemaName,
    schema,
    metadata
) {
  const command = alterTable(dialect, tableName, dbSchemaName, schema, metadata)
  return command.length === 0 ? Promise.resolve() : dialect.db.execute(command)
}

function getReferencedTableName($ref) {
  const re = /^\#\/definitions\/(.*)/
  const match = re.exec($ref)
  if (match) {
    return match[1]
  }
  return $ref
}

function equalDefinitions(was, is) {
  return (
    (was.type === is.type &&
      was.maxLength === is.maxLength &&
      was.decimals === is.decimals) ||
    (was.type === 'integer' &&
      is.type === 'number' &&
      (is.decimals === void 0 || is.decimals === 0)) ||
    (was.type === 'datetime' &&
      (is.type === 'date' ||
        (is.type === 'string' && is.format === 'date-time'))) ||
    (was.type === 'text' && (is.type === 'string' && is.maxLength === void 0))
  )
}

function canAlterColumn(from, to) {
  return (
    (from.type === to.type &&
      (from.type === 'string' && from.maxLength < to.maxLength)) ||
    (from.type === 'number' &&
      from.maxLength < to.maxLength &&
      from.decimals <= to.decimals &&
      to.maxLength - from.maxLength >= to.decimals - from.decimals) ||
    (from.type === 'string' && to.type === 'text')
  )
}

function buildPkConstraintName(tableName, primaryKey) {
  let constraintName = 'PK__' + tableName
  primaryKey.forEach(function(column) {
    constraintName += '__' + column
  })
  return constraintName
}

function buildUniqueConstraintName(tableName, unique) {
  let constraintName = 'UK__' + tableName
  unique.forEach(function(column) {
    constraintName += '__' + column
  })
  return constraintName
}

function uniqueKeyExists(key, existentKeys) {
  let found = false
  const hash = key.join('').toLowerCase()
  _.forEach(existentKeys, function(uk) {
    const ukHash = uk
        .reduce(function(columns, column) {
          return columns + column.name
        }, '')
        .toLowerCase()
    if (hash === ukHash) {
      found = true
      return false
    }
  })
  return found
}

function wrapError(error, tableName) {
  error.message = error.message + ' (' + tableName + ')'
}
