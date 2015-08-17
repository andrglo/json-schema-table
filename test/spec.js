import jsonSchemaTable from '../lib';
import personSchema from './schemas/person.json';
import clientSchema from './schemas/client.json';
import façadeSchema from './schemas/façade.json';
import personFaçadeSchema from './schemas/personFaçade.json';
import taxSchema from './schemas/tax.json';
import chai from 'chai';
import _ from 'lodash';

var expect = chai.expect;
chai.should();
let log = console.log;

function checkColumns(columns, schema) {
  expect(Object.keys(columns).length).to.equal(Object.keys(schema.properties).length);
  _.forEach(schema.properties, function(property, name) {
    let columnName = property.field || name;
    columns.should.have.property(columnName);
    if (property.type === 'number' && !property.decimals) {
      expect(columns[columnName].type).to.equal('integer');
      expect(columns[columnName].maxLength).to.equal(undefined);
    } else if (property.type === 'date') {
      expect(columns[columnName].type === 'date' ||
        columns[columnName].type === 'datetime').to.equal(true);
      expect(columns[columnName].maxLength).to.equal(undefined);
    } else if (property.type === 'object' || property.type === void 0) {
      //should be the type of the primary key of the referenced table
    } else {
      expect(columns[columnName].type).to.equal(property.type, 'Column ' + columnName);
      expect(columns[columnName].maxLength).to.equal(property.maxLength, 'Column ' + columnName);
    }
    let required = property.required || property.primaryKey ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1) ||
      (schema.required && schema.required.indexOf(name) !== -1) ||
      false;
    expect(columns[columnName].required || false).to.equal(required, 'Column ' + columnName);
  });
}

function checkForeignKey(fks, fk, table, column) {
  fk = _.find(fks, 'column', fk);
  expect(fk).to.be.a('object');
  fk.should.have.property('references');
  fk.references.should.have.property('table');
  expect(fk.references.table).to.equal(table);
  fk.references.should.have.property('column');
  expect(fk.references.column).to.equal(column);
}

export default function(db) {

  describe('table with no references', function() {
    it('should create person', function(done) {
      let person = jsonSchemaTable('person', personSchema, {db: db});
      person.sync()
        .then(function() {
          return person.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              metadata.should.have.property('primaryKey');
              expect(metadata.primaryKey).to.be.a('array');
              expect(metadata.primaryKey.length).to.equal(1);
              expect(metadata.primaryKey[0]).to.equal('personId');
              metadata.should.have.property('columns');
              expect(metadata.columns).to.be.a('object');
              checkColumns(metadata.columns, personSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should not create client due property with type array', function(done) {
      let client = jsonSchemaTable('client', clientSchema, {db: db});
      client.sync()
        .then(function() {
          done(new Error('Invalid table created'));
        })
        .catch(function(error) {
          expect(error.message.indexOf('not yet implemented') !== -1).to.equal(true);
          done();
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create client', function(done) {
      delete clientSchema.properties.taxes;
      let client = jsonSchemaTable('client', clientSchema, {db: db});
      client.sync()
        .then(function() {
          return client.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              metadata.should.have.property('primaryKey');
              expect(metadata.primaryKey).to.be.a('array');
              expect(metadata.primaryKey.length).to.equal(1);
              expect(metadata.primaryKey[0]).to.equal('clientId');
              metadata.should.have.property('columns');
              expect(metadata.columns).to.be.a('object');
              let createdSchema = _.extend({}, clientSchema);
              createdSchema.properties = _.pick(clientSchema.properties, [
                'id', 'initials', 'sales'
              ]);
              checkColumns(metadata.columns, createdSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create façade', function(done) {
      let façade = jsonSchemaTable('façade', façadeSchema, {db: db});
      façade.sync()
        .then(function() {
          return façade.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              metadata.should.have.property('primaryKey');
              expect(metadata.primaryKey).to.be.a('array');
              expect(metadata.primaryKey.length).to.equal(1);
              expect(metadata.primaryKey[0]).to.equal('Nome');
              metadata.should.have.property('columns');
              expect(metadata.columns).to.be.a('object');
              checkColumns(metadata.columns, façadeSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create person façade', function(done) {
      let personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {db: db});
      personFaçade.sync()
        .then(function() {
          return personFaçade.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              metadata.should.have.property('primaryKey');
              expect(metadata.primaryKey).to.be.a('array');
              expect(metadata.primaryKey.length).to.equal(2);
              expect(metadata.primaryKey[0]).to.equal('person');
              expect(metadata.primaryKey[1]).to.equal('XfaçadeX');
              metadata.should.have.property('columns');
              expect(metadata.columns).to.be.a('object');
              checkColumns(metadata.columns, personFaçadeSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create person tax', function(done) {
      let tax = jsonSchemaTable('tax', taxSchema, {db: db});
      tax.sync()
        .then(function() {
          return tax.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              metadata.should.have.property('primaryKey');
              expect(metadata.primaryKey).to.be.a('array');
              expect(metadata.primaryKey.length).to.equal(2);
              expect(metadata.primaryKey[0]).to.equal('city');
              expect(metadata.primaryKey[1]).to.equal('state');
              metadata.should.have.property('columns');
              expect(metadata.columns).to.be.a('object');
              checkColumns(metadata.columns, taxSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
  });

  describe('table with references if the reference exists', function() {
    it('should not alter person due table state not exists', function(done) {
      let person = jsonSchemaTable('person', personSchema, {db: db});
      person.sync({references: true})
        .then(function() {
          return person.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should not add foreign key to table client due tax has two columns as primary keys', function(done) {
      let client = jsonSchemaTable('client', clientSchema, {db: db});
      client.sync({references: true})
        .then(function() {
          done(new Error('Invalid reference created'));
        })
        .catch(function(error) {
          expect(error.message.indexOf('primary key with only one field') !== -1).to.equal(true);
          done();
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('then lets alter tax to add a single column primary key', function(done) {
      taxSchema.properties.id = {type: 'integer', autoIncrement: true, primaryKey: true}
      taxSchema.required = taxSchema.primaryKey;
      delete taxSchema.primaryKey;
      let tax = jsonSchemaTable('tax', taxSchema, {db: db});
      tax.sync({references: true})
        .then(function() {
          return tax.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              metadata.should.have.property('primaryKey');
              expect(metadata.primaryKey).to.be.a('array');
              expect(metadata.primaryKey.length).to.equal(1);
              expect(metadata.primaryKey[0]).to.equal('id');
              metadata.should.have.property('columns');
              expect(metadata.columns).to.be.a('object');
              checkColumns(metadata.columns, taxSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should add 4 foreign key to table client', function(done) {
      let client = jsonSchemaTable('client', clientSchema, {db: db});
      client.sync({references: true})
        .then(function() {
          return client.metadata()
            .then(function(metadata) {
              metadata.should.have.property('foreignKeys');
              expect(metadata.foreignKeys).to.be.a('array');
              expect(metadata.foreignKeys.length).to.equal(4);
              checkForeignKey(metadata.foreignKeys, 'clientId', 'person', 'personId');
              checkForeignKey(metadata.foreignKeys, 'cityTax', 'tax', 'id');
              checkForeignKey(metadata.foreignKeys, 'stateTax', 'tax', 'id');
              checkForeignKey(metadata.foreignKeys, 'tax', 'tax', 'id');
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should not add foreign key to table façade', function(done) {
      let façade = jsonSchemaTable('façade', façadeSchema, {db: db});
      façade.sync({references: true})
        .then(function() {
          return façade.metadata()
            .then(function(metadata) {
              metadata.should.not.have.property('foreignKeys');
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should add 2 foreign keys to table personFaçade', function(done) {
      let personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {db: db});
      personFaçade.sync({references: true})
        .then(function() {
          return personFaçade.metadata()
            .then(function(metadata) {
              metadata.should.have.property('foreignKeys');
              expect(metadata.foreignKeys).to.be.a('array');
              expect(metadata.foreignKeys.length).to.equal(2);
              checkForeignKey(metadata.foreignKeys, 'XfaçadeX', 'façade', 'Nome');
              checkForeignKey(metadata.foreignKeys, 'person', 'person', 'personId');
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });

    describe('try to alter a column type that is not permitted', function() {
      it('should not alter person column dateOfBirth', function(done) {
        personSchema.properties.dateOfBirth.type = 'string';
        let person = jsonSchemaTable('person', personSchema, {db: db});
        person.sync({references: true})
          .then(function() {
            done(new Error('Invalid alter table'));
          })
          .catch(function(error) {
            expect(error.message.indexOf('dateOfBirth cannot be modified') !== -1).to.equal(true);
            done();
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should not alter client column initials to a small string', function(done) {
        clientSchema.properties.initials.maxLength = 2;
        let client = jsonSchemaTable('client', clientSchema, {db: db});
        client.sync({references: true})
          .then(function() {
            done(new Error('Invalid alter table'));
          })
          .catch(function(error) {
            expect(error.message.indexOf('initials cannot be modified') !== -1).to.equal(true);
            done();
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should alter client column initials to a bigger string', function(done) {
        clientSchema.properties.initials.maxLength = 21;
        let client = jsonSchemaTable('client', clientSchema, {db: db});
        client.sync({references: true})
          .then(function() {
            return client.metadata()
              .then(function(metadata) {
                metadata.should.have.property('columns');
                expect(metadata.columns).to.be.a('object');
                checkColumns(metadata.columns, clientSchema);
                done();
              });
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should not alter client column sales to a small number of decimals', function(done) {
        clientSchema.properties.sales.decimals = 4;
        let client = jsonSchemaTable('client', clientSchema, {db: db});
        client.sync({references: true})
          .then(function() {
            done(new Error('Invalid alter table'));
          })
          .catch(function(error) {
            expect(error.message.indexOf('sales cannot be modified') !== -1).to.equal(true);
            done();
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should not alter client column sales to a bigger number of decimals', function(done) {
        clientSchema.properties.sales.decimals = 8;
        let client = jsonSchemaTable('client', clientSchema, {db: db});
        client.sync({references: true})
          .then(function() {
            done(new Error('Invalid alter table'));
          })
          .catch(function(error) {
            expect(error.message.indexOf('sales cannot be modified') !== -1).to.equal(true);
            done();
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should not alter client column sales to a bigger number of decimals with no equivalent greater length', function(done) {
        clientSchema.properties.sales.maxLength = 21;
        let client = jsonSchemaTable('client', clientSchema, {db: db});
        client.sync({references: true})
          .then(function() {
            done(new Error('Invalid alter table'));
          })
          .catch(function(error) {
            expect(error.message.indexOf('sales cannot be modified') !== -1).to.equal(true);
            done();
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should alter client column sales to a bigger number of decimals and length', function(done) {
        clientSchema.properties.sales.maxLength = 22;
        let client = jsonSchemaTable('client', clientSchema, {db: db});
        client.sync({references: true})
          .then(function() {
            return client.metadata()
              .then(function(metadata) {
                metadata.should.have.property('columns');
                expect(metadata.columns).to.be.a('object');
                checkColumns(metadata.columns, clientSchema);
                done();
              });
          })
          .catch(function(error) {
            done(error);
          });
      });
    });

  });

}
