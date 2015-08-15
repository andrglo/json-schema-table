import _ from 'lodash';
import assert from 'assert';

export default jsonSchemaTable;

let log = console.log;
//let log = function() {};

function jsonSchemaTable(tableName, schema, config) {
  config = config || {};
  assert(config.db, 'Database connector not informed, should be one of: mssql or pg-promise');
  let dialect = whichDialect(config.db);
  let db = getDriverAbstractionLayer(dialect, config.db);
  return {
    sync: function(options) {
      options = _.defaults({
        columns: true
      }, options);
      return getDbMetadata(dialect, tableName, schema, db)
        .then(function(metadata) {
          return (tableExists(metadata) ?
            checkTableStructure(dialect, tableName, schema, db) :
            db.execute(createTable(dialect, tableName, schema)))
            .then(function() {
              return options.references === true ?
                createTableReferences(dialect, tableName, schema, metadata, db) :
                void 0;
            });
        });
    },
    metadata: function() {
      return getDbMetadata(dialect, tableName, schema, db)
        .then(function(metadata) {
          let tableMetadata = {columns: metadata.columns};
          let primaryKeys = metadata.tablesWithPrimaryKeys[tableName];
          if (primaryKeys) {
            tableMetadata.primaryKeys = primaryKeys.map(function(primaryKey) {
              return primaryKey.name;
            });
          }
          let foreignKeys = metadata.tablesWithForeignKeys[tableName];
          if (foreignKeys) {
            tableMetadata.foreignKeys = foreignKeys.map(function(foreignKey) {
              return {
                column: foreignKey.name,
                table: foreignKey.table
              };
            });
          }
          return tableMetadata;
        });
    }
  };
}

function getDriverAbstractionLayer(dialect, db) {
  assert(dialect, 'Dialect "' + dialect + '" not supported');
  return dialect === 'postgres' ? postgresDbConnector(db) : mssqlDbConnector(db);
}

function postgresDbConnector(db) {
  return {
    execute: function(command) {
      return db.none(command);
    },
    query: function(command) {
      return db.query(command);
    }
  };
}

function mssqlDbConnector(db) {
  return {
    execute: function(command) {
      //log('execute', command);
      return (new db.Request()).batch(command);
    },
    query: function(command) {
      //log('query', command);
      return (new db.Request()).query(command);
    }
  };
}

function whichDialect(db) {
  return isNodeMssql(db) ? 'mssql' : isPostgres(db) ? 'postgres' : void 0;
}

function isNodeMssql(db) {
  return db.DRIVERS !== void 0; //todo identify in a better way
}

function isPostgres(db) {
  return db.oneOrNone !== void 0; //todo identify in a better way
}

function createTable(dialect, tableName, schema) {
  return dialect === 'mssql' ? createMssqlTable(tableName, schema) :
    createPostgresTable(tableName, schema);
}
function propertyToMssql(property, name, schema) {
  let column;
  switch (property.type) {
    case 'integer':
      column = 'INT';
      if (property.autoIncrement === true) {
        column += ' IDENTITY(1,1)';
      }
      break;
    case 'text':
    case 'string':
      column = 'NVARCHAR(' + (property.maxLength ? property.maxLength : 'MAX') + ')';
      break;
    case 'date':
    case 'datetime':
      column = 'DATETIME2';
      break;
    case 'number':
      if (property.decimals) {
        column = 'DEC(' + property.maxLength + ',' + property.decimals + ')';
      } else {
        column = 'INT';
      }
      break;
    default:
      throw new Error('Property ' + name + ' has no correspondent mssql type');
  }
  column = ' ' + column + ' ' + (property.required === true ||
    property.autoIncrement || property.primaryKey ||
    (schema.required && schema.required.indexOf(name) !== -1) ?
      'NOT NULL' : 'NULL');
  return column;
}

function mssqlToProperty(metadata, name) {
  let property = {};
  switch (metadata.DATA_TYPE) {
    case 'int':
      property.type = 'integer';
      break;
    case 'nvarchar':
      if (metadata.CHARACTER_MAXIMUM_LENGTH === -1) {
        property.type = 'text';
      } else {
        property.type = 'string';
        property.maxLength = metadata.CHARACTER_MAXIMUM_LENGTH;
      }
      break;
    case 'datetime':
    case 'datetime2':
      property.type = 'datetime';
      break;
    case 'decimal':
      property.type = 'number';
      property.maxLength = metadata.NUMERIC_PRECISION;
      property.decimals = metadata.NUMERIC_SCALE;
      break;
    default:
      throw new Error('Mssql column ' + name + ' has no correspondent property type');
  }
  if (metadata.IS_NULLABLE === 'NO') {
    property.required = true;
  }
  return property;
}

function propertyToPostgres(property, name, schema) {
  let column;
  switch (property.type) {
    case 'integer':
      if (property.autoIncrement === true) {
        column = 'SERIAL';
      } else {
        column = 'INTEGER';
      }
      break;
    case 'text':
    case 'string':
      if (property.maxLength) {
        column = 'VARCHAR(' + property.maxLength + ')';
      } else {
        column = 'TEXT';
      }
      break;
    case 'date':
      column = 'DATE';
      break;
    case 'time':
      column = 'TIME';
      if (!(property.timezone === 'UTC' || property.timezone === 'ignore')) {
        column += ' WITH TIME ZONE';
      }
      break;
    case 'datetime':
      column = 'TIMESTAMP';
      if (!(property.timezone === 'UTC' || property.timezone === 'ignore')) {
        column += ' WITH TIME ZONE';
      }
      break;
    case 'number':
      if (property.decimals) {
        column = 'NUMERIC(' + property.maxLength + ',' + property.decimals + ')';
      } else {
        column = 'INTEGER';
      }
      break;
    default:
      throw new Error('Property ' + name + ' has no correspondent postgres type');
  }
  column = ' ' + column + ' ' + (property.required === true ||
    property.autoIncrement || property.primaryKey ||
    (schema.required && schema.required.indexOf(name) !== -1) ?
      'NOT NULL' : 'NULL');
  return column;
}
function createMssqlTable(tableName, schema) {
  let columns = [];
  let primaryKeys = [];
  let constraintName = 'PK__' + tableName;
  _.forEach(schema.properties, function(property, name) {
    let fieldName = '[' + (property.field || name) + ']';
    columns.push(fieldName + ' ' + propertyToMssql(property, name, schema));
    if (property.primaryKey) {
      primaryKeys.push(fieldName);
      constraintName += '__' + name;
    }
  });
  assert(primaryKeys.length, 'Table ' + tableName + ' has no primary keys');
  columns.push('CONSTRAINT [' + constraintName + '] PRIMARY KEY (' + primaryKeys.join(',') + ')');
  return 'IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE ' +
    'TABLE_SCHEMA = \'dbo\' AND  TABLE_NAME = \'' + tableName + '\')) ' +
    'CREATE TABLE [' + tableName + '] (' + columns.join(',') + ')';
}

function createPostgresTable(tableName, schema) {
  let columns = [];
  _.forEach(schema.properties, function(property, name) {
    columns.push('"' + (property.field || name) + '"' +
      propertyToPostgres(property, name, schema));
  });
  return 'CREATE TABLE IF NOT EXISTS "' + tableName + '" (' +
    columns.join(',') + ')';
}
function createTableReferences(dialect, tableName, schema, metadata, db) {
  let command = dialect === 'mssql' ?
    createMssqlTableReferences(tableName, schema, metadata) :
    createPostgresTableReferences(tableName, schema, metadata);
  return command ? db.execute(command) : void 0;
}
function createMssqlTableReferences(tableName, schema, metadata) {
  let commands = [];
  _.forEach(schema.properties, function(property, name) {
    if (property.$ref) {
      let foreignKey = property.field || name;
      let referenceTableName = property.$ref;
      if (metadata.tablesWithPrimaryKeys[referenceTableName] && !(metadata.tablesWithForeignKeys[tableName] &&
        _.find(metadata.tablesWithForeignKeys[tableName], 'name', name))) {
        let referenceColumnName = metadata
          .tablesWithPrimaryKeys[referenceTableName][0].column;
        //todo? multiple column references
        let constraintName = 'FK__' + tableName + '__' + name;
        var cmd = 'ALTER TABLE [' + tableName +
          '] WITH NOCHECK ADD CONSTRAINT ' + constraintName + ' FOREIGN KEY ([' + foreignKey +
          ']) REFERENCES [' + referenceTableName + ']([' +
          referenceColumnName + '])';
        commands.push(cmd);
      }
    }
  });
  return commands.length ? commands.join((';')) : void 0;
}

function createPostgresTableReferences(tableName, schema, metadata) {
}

function getDbMetadata(dialect, tableName, schema, db) {
  return dialect === 'mssql' ? getMssqlMetadata(tableName, schema, db) :
    getPostgresMetadata(tableName, schema, db);
}

function getMssqlMetadata(tableName, schema, db) {
  let metadata = {
    tablesWithPrimaryKeys: {},
    tablesWithForeignKeys: {},
    columns: {}
  };
  return db.query('SELECT CONSTRAINT_NAME,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION ' +
    'FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ORDER BY CONSTRAINT_NAME,' +
    'ORDINAL_POSITION')
    .then(function(recordset) {
      recordset.map(function(record) {
        let name = getPropertyName(record.CONSTRAINT_NAME, record.ORDINAL_POSITION);
        log('cn', record.CONSTRAINT_NAME, record.COLUMN_NAME, name, schema.properties)
        if (record.CONSTRAINT_NAME.startsWith('PK__')) {
          metadata.tablesWithPrimaryKeys[record.TABLE_NAME] =
            metadata.tablesWithPrimaryKeys[record.TABLE_NAME] || [];
          metadata.tablesWithPrimaryKeys[record.TABLE_NAME].push({
            name: name,
            column: record.COLUMN_NAME
          });
        } else if (record.CONSTRAINT_NAME.startsWith('FK__')) {
          metadata.tablesWithForeignKeys[record.TABLE_NAME] =
            metadata.tablesWithForeignKeys[record.TABLE_NAME] || [];
          metadata.tablesWithForeignKeys[record.TABLE_NAME].push({
            name: name,
            column: record.COLUMN_NAME,
            table: schema.properties[name].$ref
          });
        }
      });
      return db.query('SELECT COLUMN_NAME,IS_NULLABLE,DATA_TYPE,' +
        'CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE FROM ' +
        'INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=\'' + tableName + '\'');
    })
    .then(function(recordset) {
      recordset.map(function(record) {
        var propertyName = getSchemaPropertyName(schema, record.COLUMN_NAME);
        metadata.columns[propertyName] =
          mssqlToProperty(record, propertyName);
      });
      return metadata;
    });
}

function getPostgresMetadata(tableName, schema, db) {
  let metadata = {
    tablesWithPrimaryKeys: {},
    tablesWithForeignKeys: {},
    columns: {}
  };
  return Promise.resolve(metadata);
}

function tableExists(metadata) {
  return !_.isEmpty(metadata.columns);
}

function checkTableStructure(dialect, tableName, metadata, db) {
  let command = alterTable(dialect, tableName, metadata);
  return command.length === 0 ? Promise.resolve() : db.execute(command);
}

function alterTable(dialect, tableName, schema, metadata) {
  return dialect === 'mssql' ? alterMssqlTable(tableName, schema, metadata) :
    alterPostgresTable(tableName, schema, metadata);
}

function alterMssqlTable(tableName, schema, metadata) {
  //let columns = [];
  //_.forEach(schema.properties, function(property, name) {
  //  columns.push('[' + (property.field || name) + ']' +
  //    propertyToMssql(property, name, schema));
  //});
  //return 'IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE ' +
  //  'TABLE_SCHEMA = \'dbo\' AND  TABLE_NAME = \'' + tableName + '\')) ' +
  //  'CREATE TABLE [' + tableName + '] (' + columns.join(',') + ')';
  return '';
}

function alterPostgresTable(tableName, schema, metadata) {
  //let columns = [];
  //_.forEach(schema.properties, function(property, name) {
  //  columns.push('"' + (property.field || name) + '"' +
  //    propertyToPostgres(property, name, schema));
  //});
  //return 'CREATE TABLE IF NOT EXISTS "' + tableName + '" (' +
  //  columns.join(',') + ')';
  return '';
}

function getSchemaPropertyName(schema, columnName) {
  let propertyName = columnName;
  _.forEach(schema.properties, function(property, name) {
    if (property.field === columnName) {
      propertyName = name;
      return false;
    }
  });
  return propertyName;
}

function getPropertyName(constraint, order) {
  let properties = [];
  const re = /^[PF]K__.*?__(.*)/;
  let match = re.exec(constraint);
  if (match) {
    properties = match[1].split('__');
  }
  return properties[order - 1];
}
