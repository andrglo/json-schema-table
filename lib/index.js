import _ from 'lodash';

export default jsonSchemaTable;

function jsonSchemaTable(schemaName, schema, config) {

  if (config === 'test') {
    _.forEach(['a', 'b'], function (value) {
      console.log(value);
    });
  }
}
