import jsonSchemaTable from '../lib';
import personSchema from './schemas/person.json';
import clientSchema from './schemas/client.json';
import façadeSchema from './schemas/façade.json';
import personFaçadeSchema from './schemas/personFaçade.json';
import chai from 'chai';
import _ from 'lodash';

var expect = chai.expect;
chai.should();
//let log = console.log;

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
    } else {
      expect(columns[columnName].type).to.equal(property.type);
      expect(columns[columnName].maxLength).to.equal(property.maxLength);
    }
    let required = property.required || property.primaryKey ||
      (schema.required && schema.required.indexOf(name) !== -1);
    expect(columns[columnName].required || false).to.equal(required);
  });
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
    it('should create client', function(done) {
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
              checkColumns(metadata.columns, clientSchema);
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
    it('should add foreign key to table client', function(done) {
      let client = jsonSchemaTable('client', clientSchema, {db: db});
      client.sync({references: true})
        .then(function() {
          return client.metadata()
            .then(function(metadata) {
              metadata.should.have.property('foreignKeys');
              expect(metadata.foreignKeys).to.be.a('array');
              expect(metadata.foreignKeys.length).to.equal(1);
              let fk1 = metadata.foreignKeys[0];
              expect(fk1).to.be.a('object');
              fk1.should.have.property('references');
              fk1.references.should.have.property('table');
              expect(fk1.references.table).to.equal('person');
              fk1.references.should.have.property('column');
              expect(fk1.references.column).to.equal('personId');
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
              let fk1 = _.find(metadata.foreignKeys, 'column', 'XfaçadeX');
              expect(fk1).to.be.a('object');
              fk1.should.have.property('references');
              fk1.references.should.have.property('table');
              expect(fk1.references.table).to.equal('façade');
              fk1.references.should.have.property('column');
              expect(fk1.references.column).to.equal('Nome');
              let fk2 = _.find(metadata.foreignKeys, 'column', 'person');
              expect(fk2).to.be.a('object');
              fk2.should.have.property('references');
              fk2.references.should.have.property('table');
              expect(fk2.references.table).to.equal('person');
              fk2.references.should.have.property('column');
              expect(fk2.references.column).to.equal('personId');
              done();
            });
        })
        .catch(function(error) {
          done(error);
        });
    });
  });

}
