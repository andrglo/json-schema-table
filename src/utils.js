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
