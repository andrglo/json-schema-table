var jsonSchemaTable = require('..');
var chai = require('chai');
var _ = require('lodash');

var personSchema = require('./schemas/person.json');
var clientSchema = require('./schemas/client.json');
var façadeSchema = require('./schemas/façade.json');
var personFaçadeSchema = require('./schemas/personFaçade.json');
var taxSchema = require('./schemas/tax.json');

var modifiedClientSchema;
var modifiedPersonSchema;
var modifiedTaxSchema;

var expect = chai.expect;
chai.should();
var log = console.log;

function checkColumns(columns, schema) {
  expect(Object.keys(columns).length).to.equal(Object.keys(schema.properties).length);
  _.forEach(schema.properties, function(property, name) {
    var columnName = property.field || name;
    columns.should.have.property(columnName);
    if (property.type === 'number' && !property.decimals) {
      expect(columns[columnName].type).to.equal('integer');
      expect(columns[columnName].maxLength).to.equal(undefined);
    } else if (property.type === 'date') {
      expect(columns[columnName].type === 'date' ||
        columns[columnName].type === 'datetime').to.equal(true);
      expect(columns[columnName].maxLength).to.equal(undefined);
    } else if (property.type === 'string' && property.maxLength === void 0) {
      expect(columns[columnName].type === 'text').to.equal(true);
      expect(columns[columnName].maxLength).to.equal(undefined);
    } else if (property.type === 'object' || property.type === void 0) {
      //should be the type of the primary key of the referenced table
    } else {
      expect(columns[columnName].type).to.equal(property.type, 'Column ' + columnName);
      expect(columns[columnName].maxLength).to.equal(property.maxLength, 'Column ' + columnName);
    }
    var required = property.required || property.primaryKey || property.unique ||
      (schema.primaryKey && schema.primaryKey.indexOf(name) !== -1) ||
      (schema.required && schema.required.indexOf(name) !== -1) ||
      (schema.unique && isInUniqueProperty(name, schema.unique)) ||
      false;
    expect(columns[columnName].required || false).to.equal(required, 'Column ' + columnName);
  });
}

function isInUniqueProperty(column, unique) {
  for (var i = 0; i < unique.length; i++) {
    var columns = unique[i];
    if (columns.indexOf(column) !== -1) {
      return true;
    }
  }
  return false;
}

function checkForeignKey(fks, fk, table, column) {
  fk = _.find(fks, 'column', fk);
  expect(fk).to.be.a('object');
  fk.should.have.property('references');
  fk.references.should.have.property('table');
  expect(fk.references.table).to.equal(table);
  fk.references.should.have.property('column');
  expect(fk.references.column.toLowerCase()).to.equal(column.toLowerCase());
}

module.exports = function(db) {

  describe('table with no references', function() {
    it('should not sync person before its created', function(done) {
      var person = jsonSchemaTable('person', personSchema, {db: db});
      person.sync()
        .then(function() {
          done(new Error('Invalid table synced'));
        })
        .catch(function(error) {
          expect(error.message.indexOf('tables should be created first') !== -1).to.equal(true);
          done();
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create person', function(done) {
      var person = jsonSchemaTable('person', personSchema, {db: db});
      person.create()
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
      var client = jsonSchemaTable('client', clientSchema, {db: db});
      client.create()
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
      modifiedClientSchema = _.cloneDeep(clientSchema);
      delete modifiedClientSchema.properties.taxes;
      var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
      client.create()
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
              var createdSchema = _.extend({}, modifiedClientSchema);
              createdSchema.properties = _.pick(modifiedClientSchema.properties, [
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
      var façade = jsonSchemaTable('façade', façadeSchema, {db: db});
      façade.create()
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
      var personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {db: db});
      personFaçade.create()
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
      var tax = jsonSchemaTable('tax', taxSchema, {db: db});
      tax.create()
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
      var person = jsonSchemaTable('person', personSchema, {db: db});
      person.sync()
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
      var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
      client.sync()
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
      modifiedTaxSchema = _.cloneDeep(taxSchema);
      modifiedTaxSchema.properties.id = {type: 'integer', autoIncrement: true, primaryKey: true}
      modifiedTaxSchema.required = modifiedTaxSchema.primaryKey;
      delete modifiedTaxSchema.primaryKey;
      var tax = jsonSchemaTable('tax', modifiedTaxSchema, {db: db});
      tax.sync()
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
              checkColumns(metadata.columns, modifiedTaxSchema);
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should add 4 foreign key to table client', function(done) {
      var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
      client.sync()
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
      var façade = jsonSchemaTable('façade', façadeSchema, {db: db});
      façade.sync()
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
      var personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {db: db});
      personFaçade.sync()
        .then(function() {
          return personFaçade.metadata()
            .then(function(metadata) {
              metadata.should.have.property('foreignKeys');
              expect(metadata.foreignKeys).to.be.a('array');
              expect(metadata.foreignKeys.length).to.equal(2);
              checkForeignKey(metadata.foreignKeys, 'XfaçadeX', 'façade', 'Nome');
              checkForeignKey(metadata.foreignKeys, 'person', 'person', 'fieldName');
              done();
            });
        })
        .catch(function(error) {
          log(error)
          done(error);
        });
    });

    describe('modify structure', function() {
      it('should not alter person column dateOfBirth', function(done) {
        modifiedPersonSchema = _.cloneDeep(personSchema);
        modifiedPersonSchema.properties.dateOfBirth.type = 'string';
        var person = jsonSchemaTable('person', modifiedPersonSchema, {db: db});
        person.sync()
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
        modifiedClientSchema.properties.initials.maxLength = 2;
        var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
        client.sync()
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
        modifiedClientSchema.properties.initials.maxLength = 21;
        var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
        client.sync()
          .then(function() {
            return client.metadata()
              .then(function(metadata) {
                metadata.should.have.property('columns');
                expect(metadata.columns).to.be.a('object');
                checkColumns(metadata.columns, modifiedClientSchema);
                done();
              });
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should not alter client column sales to a small number of decimals', function(done) {
        modifiedClientSchema.properties.sales.decimals = 4;
        var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
        client.sync()
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
        modifiedClientSchema.properties.sales.decimals = 8;
        var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
        client.sync()
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
        modifiedClientSchema.properties.sales.maxLength = 21;
        var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
        client.sync()
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
        modifiedClientSchema.properties.sales.maxLength = 22;
        var client = jsonSchemaTable('client', modifiedClientSchema, {db: db});
        client.sync()
          .then(function() {
            return client.metadata()
              .then(function(metadata) {
                metadata.should.have.property('columns');
                expect(metadata.columns).to.be.a('object');
                checkColumns(metadata.columns, modifiedClientSchema);
                done();
              });
          })
          .catch(function(error) {
            done(error);
          });
      });
      it('should alter person column name to text', function(done) {
        modifiedPersonSchema.properties.dateOfBirth.type = 'date';
        modifiedPersonSchema.properties.name.type = 'text';
        delete modifiedPersonSchema.properties.name.type;
        var person = jsonSchemaTable('person', modifiedPersonSchema, {db: db});
        person.sync()
          .then(function() {
            return person.metadata()
              .then(function(metadata) {
                metadata.should.have.property('columns');
                expect(metadata.columns).to.be.a('object');
                checkColumns(metadata.columns, modifiedPersonSchema);
                done();
              });
          })
          .catch(function(error) {
            done(error);
          });
      });
    });

  });

};
