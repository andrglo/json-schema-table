import jsonSchemaTable from '../lib';
import personSchema from './schemas/person.json';
import clientSchema from './schemas/client.json';
import façadeSchema from './schemas/façade.json';
import personFaçadeSchema from './schemas/personFaçade.json';
import chai from 'chai';

var expect = chai.expect;
var should = chai.should();
let log = console.log;

export default function(db) {

  describe('table with no references', function() {
    it('should create person', function(done) {
      let person = jsonSchemaTable('person', personSchema, {db: db});
      person.sync()
        .then(function() {
          done();
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create client', function(done) {
      let client = jsonSchemaTable('client', clientSchema, {db: db});
      client.sync()
        .then(function() {
          done();
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create façade', function(done) {
      let façade = jsonSchemaTable('façade', façadeSchema, {db: db});
      façade.sync()
        .then(function() {
          done();
        })
        .catch(function(error) {
          done(error);
        });
    });
    it('should create person façade', function(done) {
      let personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {db: db});
      personFaçade.sync()
        .then(function() {
          done();
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
          done();
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
              metadata.foreignKeys[0].should.have.property('column');
              expect(metadata.foreignKeys[0].column).to.equal('id');
              metadata.foreignKeys[0].should.have.property('table');
              expect(metadata.foreignKeys[0].table).to.equal('person');
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
              metadata.should.have.property('primaryKeys');
              expect(metadata.primaryKeys).to.be.a('array');
              expect(metadata.primaryKeys.length).to.equal(1);
              expect(metadata.primaryKeys[0]).to.equal('id');
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
              log(metadata)
              metadata.should.have.property('foreignKeys');
              metadata.should.have.property('primaryKeys');
              expect(metadata.primaryKeys).to.be.a('array');
              expect(metadata.primaryKeys.length).to.equal(1);
              expect(metadata.primaryKeys[0]).to.equal('id');
              done();
            });
        })
        .catch(function(error) {
          //log(error)
          done(error);
        });
    });
  });

}
