import _ from 'lodash';
import sql from 'sql';

class jsonSchemaTable extends sql.Sql {
  constructor(tableName, schema, config) {
    super(whichDialect(config.db));
    this.config = config;
  }

  sync(options) {
    options = _.defaults({
      columns: true,
      references: false
    }, options);

    console.log('sync called', options, whichDialect(this.config.db));

  }
}

export default jsonSchemaTable;

function whichDialect(db) {
  return isNodeMssql(db) ? 'mssql' : isPostgres(db) ? 'postgres' : void 0;
}

function isNodeMssql(db) {
  return db.DRIVERS !== void 0; //todo identify in a better way
}

function isPostgres(db) {
  return db.oneOrNone !== void 0; //todo identify in a better way
}
