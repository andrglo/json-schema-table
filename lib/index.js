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
      //log('execute', command);
      return db.none(command);
    },
    query: function(command) {
      //log('query', command);
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
  switch (metadata.data_type) {
    case 'int':
      property.type = 'integer';
      break;
    case 'nvarchar':
      if (metadata.character_maximum_length === -1) {
        property.type = 'text';
      } else {
        property.type = 'string';
        property.maxLength = metadata.character_maximum_length;
      }
      break;
    case 'datetime':
    case 'datetime2':
      property.type = 'datetime';
      break;
    case 'decimal':
      property.type = 'number';
      property.maxLength = metadata.numeric_precision;
      property.decimals = metadata.numeric_scale;
      break;
    default:
      throw new Error('Mssql column ' + name + ' type ' + metadata.data_type + ' has no correspondent property type');
  }
  if (metadata.is_nullable === 'NO') {
    property.required = true;
  }
  return property;
}

function propertyToPostgres(property, name, schema, isAlter) {
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
    case 'object':
    case void 0:
      return void 0;
    case 'array':
      throw new Error('Property ' + name + ' has type not yet implemented: ' + property.type);
    default:
      throw new Error('Property ' + name + ' has no correspondent postgres type');
  }
  column = ' ' + column;
  if (isAlter !== true) {
    column += ' ' +
      (property.required === true ||
      property.autoIncrement === true ||
      property.primaryKey === true ||
      (schema && schema.primaryKey && schema.primaryKey.indexOf(name) !== -1) ||
      (schema && schema.required && schema.required.indexOf(name) !== -1) ?
        'NOT NULL' : 'NULL');
  }
  return column;
}

function postgresToProperty(metadata, name) {
  let property = {};
  switch (metadata.data_type) {
    case 'integer':
    case 'text':
    case 'date':
    case 'datetime':
      property.type = metadata.data_type;
      break;
    case 'character varying':
      property.type = 'string';
      property.maxLength = metadata.character_maximum_length;
      break;
    case 'numeric':
      property.type = 'number';
      property.maxLength = metadata.numeric_precision;
      property.decimals = metadata.numeric_scale;
      break;
    default:
      throw new Error('Postgres column ' + name + ' type ' + metadata.data_type + ' has no correspondent property type');
  }
  if (metadata.is_nullable === 'NO') {
    property.required = true;
  }
  return property;
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
  let primaryKey = [];
  _.forEach(schema.properties, function(property, name) {
    let fieldName = property.field || name;
    var fieldType = propertyToPostgres(property, name, schema);
    if (fieldType === void 0) {
      return;
    }
    columns.push('"' + fieldName + '" ' + fieldType);
    if (property.primaryKey === true ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1)) {
      primaryKey.push(fieldName);
    }
  });
  assert(primaryKey.length, 'Table ' + tableName + ' has no primary keys');
  columns.push('CONSTRAINT "' + buildPkConstraintName(tableName, primaryKey) +
    '" PRIMARY KEY (' + primaryKey.map(function(column) {
      return '"' + column + '"';
    }).join(',') + ')');
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
          '] ADD CONSTRAINT ' + constraintName + ' FOREIGN KEY ([' + foreignKey +
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
        var cmd = 'ALTER TABLE "' + tableName +
          '" ADD CONSTRAINT ' + constraintName + ' FOREIGN KEY ("' + foreignKey +
          '") REFERENCES "' + referenceTableName + '"("' +
          referenceTablePrimaryKey.column + '")';
        if (!metadata.columns[foreignKey]) {
          cmd = 'ALTER TABLE "' + tableName + '" ADD "' + foreignKey + '" ' +
            propertyToPostgres(referenceTablePrimaryKey.property, foreignKey) + ';' + cmd;
        }
        commands.push(cmd);
      }
    }
  });
  return commands.length ? commands.join((';')) : void 0;
}

function getDbMetadata(dialect, tableName, schema, db) {
  let dbToProperty = dialect === 'mssql' ? mssqlToProperty : postgresToProperty;
  let metadata = {
    tablesWithPrimaryKey: {},
    tablesWithForeignKeys: {},
    columns: {}
  };
  return db.query('SELECT pk.CONSTRAINT_NAME as constraint_name,pk.TABLE_NAME as table_name,' +
    'pk.COLUMN_NAME as column_name,pk.ORDINAL_POSITION as ordinal_position,' +
    'c.DATA_TYPE as data_type,c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,' +
    'c.NUMERIC_PRECISION as numeric_precisison,c.NUMERIC_SCALE as numerico_scale ' +
    'FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk ' +
    'INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON pk.COLUMN_NAME=c.COLUMN_NAME AND pk.TABLE_NAME=c.TABLE_NAME ' +
    'ORDER BY pk.CONSTRAINT_NAME,pk.ORDINAL_POSITION')
    .then(function(recordset) {
      recordset.map(function(record) {
        record.constraint_name = record.constraint_name.toLowerCase();
        if (record.constraint_name.startsWith('pk__')) {
          let name = getPkConstraintInfo(record.constraint_name, record.ordinal_position);
          metadata.tablesWithPrimaryKey[record.table_name] =
            metadata.tablesWithPrimaryKey[record.table_name] || [];
          metadata.tablesWithPrimaryKey[record.table_name].push({
            name: name,
            column: record.column_name,
            property: dbToProperty(record, name)
          });
        } else if (record.constraint_name.startsWith('fk__')) {
          let constraintInfo = getFkConstraintInfo(record.constraint_name);
          metadata.tablesWithForeignKeys[record.table_name] =
            metadata.tablesWithForeignKeys[record.table_name] || [];
          metadata.tablesWithForeignKeys[record.table_name].push({
            column: record.column_name,
            referenceColumn: constraintInfo.referenceColumn,
            referenceTable: constraintInfo.referenceTable
          });
        }
      });
      return db.query('SELECT COLUMN_NAME as column_name,IS_NULLABLE as is_nullable,' +
        'DATA_TYPE as data_type,' +
        'CHARACTER_MAXIMUM_LENGTH as character_maximum_length,NUMERIC_PRECISION as numeric_precision,' +
        'NUMERIC_SCALE as numeric_scale FROM ' +
        'INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=\'' + tableName + '\'');
    })
    .then(function(recordset) {
      recordset.map(function(record) {
        var propertyName = getSchemaPropertyName(schema, record.column_name);
        metadata.columns[record.column_name] =
          dbToProperty(record, propertyName);
      });
      return metadata;
    });
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
  let commands = [];
  let primaryKey = [];
  _.forEach(schema.properties, function(property, name) {

    let fieldName = property.field || name;
    var fieldType = propertyToPostgres(property, name, schema, true);
    if (fieldType === void 0) {
      return;
    }
    if (property.primaryKey === true ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1)) {
      primaryKey.push(fieldName);
    }

    if (!metadata.columns[fieldName]) {
      commands.push('ALTER TABLE "' + tableName + '" ADD "'
        + fieldName + '"' + fieldType);
    } else if (!equalDefinitions(metadata.columns[fieldName], property)) {
      if (canAlterColumn(metadata.columns[fieldName], property)) {
        commands.push('ALTER TABLE "' + tableName + '" ALTER COLUMN "'
          + fieldName + '" TYPE ' + fieldType);
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
    commands.push('ALTER TABLE "' + tableName + '" DROP CONSTRAINT "'
      + buildPkConstraintName(tableName, oldPrimaryKey) + '"');
    commands.push('ALTER TABLE "' + tableName + '" ADD CONSTRAINT "'
      + buildPkConstraintName(tableName, primaryKey) + '" PRIMARY KEY (' +
      primaryKey.map(function(column) {
        return '"' + column + '"';
      }).join(',') + ')');
  }
  return commands.join(';');
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
  const re = /^pk__.*?__(.*)/;
  let match = re.exec(constraint);
  if (match) {
    properties = match[1].split('__');
  }
  return properties[order - 1];
}

function getFkConstraintInfo(constraint) {
  let constraintInfo = {};
  const re = /^fk__.*?__(.*?)__(.*?)__(.*)/;
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
