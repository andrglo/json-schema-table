/* eslint-disable no-extra-parens */
var _ = require('lodash');

function findProperty(name, properties) {
  var property = properties[name];
  if (property === void 0) {
    property = _.reduce(properties, function(res, prop, propName) {
        return res ? res : (name === propName ? prop : void 0);
      }, void 0) ||
      _.reduce(properties, function(res, prop) {
        return res ? res : (prop.field && name === prop.field ? prop : void 0);
      }, void 0);
    if (property === void 0) {
      throw new Error('Property "' + name + '" not found');
    }
  }
  return property;
}

exports.isInArray = isInArray;

function isInArray(name, array, schema) {
  var found = false;
  var property = findProperty(name, schema.properties);
  _.forEach(array, function(element) {
    if (name === element || name === property.field) {
      found = true;
      return false;
    }
  });
  return found;
}

exports.mapToColumnName = mapToColumnName;

function mapToColumnName(array, schema) {
  return array && array.map(function(name) {
    return findProperty(name, schema.properties).field || name;
  });
}

function checkConstraints(db, catalog, dbSchemaName, dbToProperty) {
  function isRedshift() {
    return new Promise(function(resolve, reject) {
      if (db.dialect !== 'postgres')
        resolve(false);
      else {
        db.isRedshift()
        .then(function(res) {
          resolve(res);
        })
        .catch(function(err) {
          reject(err);
        });
      }
    });
  }

  return isRedshift()
  .then(function(rs) {
    if (!rs) {
      return db.query('SELECT pk.CONSTRAINT_NAME as constraint_name,pk.TABLE_NAME as table_name,' +
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
        'WHERE pk.TABLE_CATALOG=' + catalog +
        'AND pk.TABLE_SCHEMA=\'' + dbSchemaName + '\'' +
        'ORDER BY pk.TABLE_NAME,pk.CONSTRAINT_NAME,pk.ORDINAL_POSITION')
        .then(function(recordset) {
          var constraints = {};
          recordset.map(function(record) {
            if (!constraints[record.constraint_name])
              constraints[record.constraint_name] = {
                table: record.table_name,
                references: record.ref_table_name,
                columns: []
              };

            var constraint = constraints[record.constraint_name];
            constraint.columns.push(dbToProperty(record));
          });
          return constraints;
        });
    } else {
      // Redshift does not support PK or FK constraints so just return an empty array.
      return Promise.resolve([]);
    }
  });
}

exports.checkConstraints = checkConstraints;
