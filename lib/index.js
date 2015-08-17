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
    create: function() {
      return Promise.resolve()
        .then(function() {
          return db.execute(createTable(dialect, tableName, schema));
        });
    },
    sync: function() {
      return getDbMetadata(dialect, tableName, schema, db)
        .then(function(metadata) {
          if (!tableExists(metadata)) {
            throw new Error('All tables should be created first');
          }
          return checkTableStructure(dialect, tableName, schema, metadata, db)
            .then(function() {
              return createTableReferences(dialect, tableName, schema, metadata, db);
            });
        });
    },
    metadata: function() {
      return getDbMetadata(dialect, tableName, schema, db)
        .then(function(metadata) {
          let tableMetadata = {columns: metadata.columns};
          let primaryKey = metadata.tablesWithPrimaryKey[tableName];
          if (primaryKey) {
            tableMetadata.primaryKey = primaryKey.map(function(column) {
              return column.column;
            });
          }
          let foreignKeys = metadata.tablesWithForeignKeys[tableName];
          if (foreignKeys) {
            tableMetadata.foreignKeys = foreignKeys.map(function(foreignKey) {
              return {
                column: foreignKey.column,
                references: {
                  table: foreignKey.referenceTable,
                  column: foreignKey.referenceColumn
                }
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
    case 'object':
    case void 0:
      return void 0;
    case 'array':
      throw new Error('Property ' + name + ' has type not yet implemented: ' + property.type);
    default:
      throw new Error('Property ' + name + ' has no correspondent mssql type');
  }
  column = ' ' + column + ' ' +
    (property.required === true || property.primaryKey === true ||
    (schema && schema.primaryKey && schema.primaryKey.indexOf(name) !== -1) ||
    (schema && schema.required && schema.required.indexOf(name) !== -1) ?
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
  column = ' ' + column + ' ' +
    (property.required === true ||
    property.autoIncrement === true ||
    property.primaryKey === true ||
    (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1) ||
    (schema.required && schema.required.indexOf(name) !== -1) ?
      'NOT NULL' : 'NULL');
  return column;
}
function createMssqlTable(tableName, schema) {
  let columns = [];
  let primaryKey = [];
  _.forEach(schema.properties, function(property, name) {
    let fieldName = property.field || name;
    var fieldType = propertyToMssql(property, name, schema);
    if (fieldType === void 0) {
      return;
    }
    columns.push('[' + fieldName + '] ' + fieldType);
    if (property.primaryKey === true ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1)) {
      primaryKey.push(fieldName);
    }
  });
  assert(primaryKey.length, 'Table ' + tableName + ' has no primary keys');
  columns.push('CONSTRAINT [' + buildPkConstraintName(tableName, primaryKey) + '] PRIMARY KEY (' + primaryKey.join(',') + ')');
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
    let $ref = property.$ref || (property.schema && property.schema.$ref);
    if ($ref) {
      let foreignKey = property.field || name;
      let referenceTableName = getReferenceTableName($ref);
      if (metadata.tablesWithPrimaryKey[referenceTableName] && !(metadata.tablesWithForeignKeys[tableName] &&
        _.find(metadata.tablesWithForeignKeys[tableName], 'column', foreignKey))) {
        assert(metadata.tablesWithPrimaryKey[referenceTableName].length === 1,
          'Table ' + referenceTableName +
          ' should have a primary key with only one field to be referenced');
        var referenceTablePrimaryKey = metadata.tablesWithPrimaryKey[referenceTableName][0];
        let constraintName = 'FK__' + tableName + '__' + foreignKey + '__' +
          referenceTableName + '__' + referenceTablePrimaryKey.column;
        var cmd = 'ALTER TABLE [' + tableName +
          '] WITH NOCHECK ADD CONSTRAINT ' + constraintName + ' FOREIGN KEY ([' + foreignKey +
          ']) REFERENCES [' + referenceTableName + ']([' +
          referenceTablePrimaryKey.column + '])';
        if (!metadata.columns[foreignKey]) {
          cmd = 'ALTER TABLE [' + tableName + '] ADD [' + foreignKey + '] ' +
            propertyToMssql(referenceTablePrimaryKey.property, foreignKey) + ';' + cmd;
        }
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
    tablesWithPrimaryKey: {},
    tablesWithForeignKeys: {},
    columns: {}
  };
  return db.query('SELECT pk.CONSTRAINT_NAME,pk.TABLE_NAME,pk.COLUMN_NAME,pk.ORDINAL_POSITION,c.DATA_TYPE,c.CHARACTER_MAXIMUM_LENGTH,c.NUMERIC_PRECISION,c.NUMERIC_SCALE ' +
    'FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk ' +
    'INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON pk.COLUMN_NAME=c.COLUMN_NAME AND pk.TABLE_NAME=c.TABLE_NAME ' +
    'ORDER BY pk.CONSTRAINT_NAME,pk.ORDINAL_POSITION')
    .then(function(recordset) {
      recordset.map(function(record) {
        if (record.CONSTRAINT_NAME.startsWith('PK__')) {
          let name = getPkConstraintInfo(record.CONSTRAINT_NAME, record.ORDINAL_POSITION);
          metadata.tablesWithPrimaryKey[record.TABLE_NAME] =
            metadata.tablesWithPrimaryKey[record.TABLE_NAME] || [];
          metadata.tablesWithPrimaryKey[record.TABLE_NAME].push({
            name: name,
            column: record.COLUMN_NAME,
            property: mssqlToProperty(record, name)
          });
        } else if (record.CONSTRAINT_NAME.startsWith('FK__')) {
          let constraintInfo = getFkConstraintInfo(record.CONSTRAINT_NAME);
          metadata.tablesWithForeignKeys[record.TABLE_NAME] =
            metadata.tablesWithForeignKeys[record.TABLE_NAME] || [];
          metadata.tablesWithForeignKeys[record.TABLE_NAME].push({
            column: record.COLUMN_NAME,
            referenceColumn: constraintInfo.referenceColumn,
            referenceTable: constraintInfo.referenceTable
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
        metadata.columns[record.COLUMN_NAME] =
          mssqlToProperty(record, propertyName);
      });
      return metadata;
    });
}

function getPostgresMetadata(tableName, schema, db) {
  let metadata = {
    tablesWithPrimaryKey: {},
    tablesWithForeignKeys: {},
    columns: {}
  };
  return Promise.resolve(metadata);
}

function tableExists(metadata) {
  return !_.isEmpty(metadata.columns);
}

function checkTableStructure(dialect, tableName, schema, metadata, db) {
  let command = alterTable(dialect, tableName, schema, metadata);
  return command.length === 0 ? Promise.resolve() : db.execute(command);
}

function alterTable(dialect, tableName, schema, metadata) {
  return dialect === 'mssql' ? alterMssqlTable(tableName, schema, metadata) :
    alterPostgresTable(tableName, schema, metadata);
}

function alterMssqlTable(tableName, schema, metadata) {
  let commands = [];
  let primaryKey = [];
  _.forEach(schema.properties, function(property, name) {

    let fieldName = property.field || name;
    var fieldType = propertyToMssql(property, name, schema);
    if (fieldType === void 0) {
      return;
    }
    if (property.primaryKey === true ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1)) {
      primaryKey.push(fieldName);
    }

    if (!metadata.columns[fieldName]) {
      commands.push('ALTER TABLE [' + tableName + '] ADD ['
        + fieldName + ']' + fieldType);
    } else if (!equalDefinitions(metadata.columns[fieldName], property)) {
      if (canAlterColumn(metadata.columns[fieldName], property)) {
        commands.push('ALTER TABLE [' + tableName + '] ALTER COLUMN ['
          + fieldName + ']' + fieldType);
      } else {
        throw new Error('Column ' + fieldName + ' cannot be modified');
      }
    }
  });
  assert(primaryKey.length, 'Table ' + tableName + ' has no primary keys');
  let oldPrimaryKey = metadata.tablesWithPrimaryKey[tableName].map(function(column) {
    return column.column;
  });
  if (_.difference(primaryKey, oldPrimaryKey).length) {
    commands.push('ALTER TABLE [' + tableName + '] DROP CONSTRAINT ['
      + buildPkConstraintName(tableName, oldPrimaryKey) + ']');
    commands.push('ALTER TABLE [' + tableName + '] ADD CONSTRAINT ['
      + buildPkConstraintName(tableName, primaryKey) + '] PRIMARY KEY (' + primaryKey.join(',') + ')');
  }

  return commands.join(';');

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

function getPkConstraintInfo(constraint, order) {
  let properties = [];
  const re = /^PK__.*?__(.*)/;
  let match = re.exec(constraint);
  if (match) {
    properties = match[1].split('__');
  }
  return properties[order - 1];
}

function getFkConstraintInfo(constraint) {
  let constraintInfo = {};
  const re = /^FK__.*?__(.*?)__(.*?)__(.*)/;
  let match = re.exec(constraint);
  if (match) {
    constraintInfo.column = match[1];
    constraintInfo.referenceTable = match[2];
    constraintInfo.referenceColumn = match[3];
  }
  return constraintInfo;
}

function getReferenceTableName($ref) {
  const re = /^\#\/definitions\/(.*)/;
  let match = re.exec($ref);
  if (match) {
    return match[1];
  }
  return $ref;
}

function equalDefinitions(was, is) {
  return (was.type === is.type &&
    was.maxLength === is.maxLength &&
    was.decimals === is.decimals) ||
    (was.type === 'integer' &&
    is.type === 'number' &&
    is.decimals === void 0) ||
    (was.type === 'datetime' && is.type === 'date');
}

function canAlterColumn(from, to) {
  return from.type === to.type &&
    (from.type === 'string' &&
    from.maxLength < to.maxLength) ||
    (from.type === 'number' &&
    from.maxLength < to.maxLength &&
    from.decimals <= to.decimals &&
    to.maxLength - from.maxLength >= to.decimals - from.decimals);
}

function buildPkConstraintName(tableName, primaryKey) {
  let constraintName = 'PK__' + tableName;
  primaryKey.forEach(function(column) {
    constraintName += '__' + column;
  });
  return constraintName;
}
