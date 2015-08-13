import _ from 'lodash';

export default jsonSchemaTable;

function jsonSchemaTable() {
  _.forEach(['a', 'b'], function(value) {
    console.log(value);
  });
}
