const jsonSchemaTable = require('../src')
const _ = require('lodash')
const utils = require('../src/utils')

const personSchema = require('./schemas/person.json')
const clientSchema = require('./schemas/client.json')
const façadeSchema = require('./schemas/façade.json')
const personFaçadeSchema = require('./schemas/personFaçade.json')
const taxSchema = require('./schemas/tax.json')
const catalogSchema = require('./schemas/catalog.json')
const reffabSchema = require('./schemas/reffab.json')
const refforfabSchema = require('./schemas/refforfab.json')

let modifiedClientSchema
let modifiedPersonSchema
let modifiedTaxSchema
let expect

function checkColumns(columns, schema) {
  expect(Object.keys(columns).length).to.equal(
      Object.keys(schema.properties).length
  )
  _.forEach(schema.properties, function(property, name) {
    const columnName = property.field || name
    columns.should.have.property(columnName)
    if (property.type === 'number' && !property.decimals) {
      expect(columns[columnName].type).to.equal('integer')
      expect(columns[columnName].maxLength).to.equal(undefined)
    } else if (property.type === 'date') {
      expect(
          columns[columnName].type === 'date' ||
          columns[columnName].type === 'date'
      ).to.equal(true)
      expect(columns[columnName].maxLength).to.equal(undefined)
    } else if (property.type === 'string' && property.maxLength === void 0) {
      expect(columns[columnName].type === 'text').to.equal(true)
      expect(columns[columnName].maxLength).to.equal(undefined)
    } else if (property.type === 'object' || property.type === void 0) {
      // should be the type of the primary key of the referenced table
    } else {
      expect(columns[columnName].type).to.equal(
          property.type,
          'Column ' + columnName
      )
      expect(columns[columnName].maxLength).to.equal(
          property.maxLength,
          'Column ' + columnName
      )
    }
    const required =
      property.required ||
      property.primaryKey ||
      utils.isInArray(name, schema.primaryKey, schema) ||
      utils.isInArray(name, schema.required, schema) ||
      false
    expect(columns[columnName].required || false).to.equal(
        required,
        'Column ' + columnName
    )
  })
}

function checkForeignKey(fks, columns, refTable, refTableColumns) {
  let found
  const joinedColumns = columns.join().toLowerCase()
  const joinedRefColumns = refTableColumns.join().toLowerCase()
  _.forEach(fks, function(fk) {
    if (fk.table.toLowerCase() === refTable.toLowerCase()) {
      const joinedFkColumns = fk.columns
          .map(function(column) {
            return column.name
          })
          .join()
          .toLowerCase()
      const joinedFkRefColumns = fk.columns
          .map(function(column) {
            return column.references
          })
          .join()
          .toLowerCase()
      if (
        joinedFkColumns === joinedColumns &&
        joinedFkRefColumns === joinedRefColumns
      ) {
        found = true
        return false
      }
    }
  })
  expect(found).to.equal(
      true,
      'Foreign key ' + columns.join() + ' don\'t have a correspondent key'
  )
}

module.exports = function(options) {
  let db
  before(function() {
    return import('chai').then(chai => {
      chai.should()
      expect = chai.expect
    })
  })
  describe('Table with no references', function() {
    it('should not sync person before its created', function(done) {
      db = options.db
      const person = jsonSchemaTable('person', personSchema, {db})
      person
          .sync()
          .then(function() {
            done(new Error('Invalid table synced'))
          })
          .catch(function(error) {
            expect(
                error.message.indexOf('tables should be created first') !== -1
            ).to.equal(true)
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create person', function(done) {
      const person = jsonSchemaTable('person', personSchema, {db})
      person
          .create()
          .then(function() {
            return person.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(1)
              expect(metadata.primaryKey[0]).to.equal('personId')

              metadata.should.have.property('uniqueKeys')
              expect(metadata.uniqueKeys).to.be.a('array')
              expect(metadata.uniqueKeys.length).to.equal(3)
              metadata.uniqueKeys.forEach(function(uk) {
                if (uk.length === 1) {
                  expect(uk[0]).to.equal('fieldName')
                } else if (uk.length === 2) {
                  expect(uk[0]).to.equal('state')
                  expect(uk[1]).to.equal('dateOfBirth')
                } else if (uk.length === 3) {
                  expect(uk[0]).to.equal('fieldName')
                  expect(uk[1]).to.equal('state')
                  expect(uk[2]).to.equal('momentOfBirth')
                } else {
                  throw new Error('Invalid unique key')
                }
              })
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, personSchema)
              done()
            })
          })
          .catch(done)
    })
    it('should not create client due property with type array', function(done) {
      const client = jsonSchemaTable('client', clientSchema, {db})
      client
          .create()
          .then(function() {
            done(new Error('Invalid table created'))
          })
          .catch(function(error) {
            expect(error.message.indexOf('not yet implemented') !== -1).to.equal(
                true
            )
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create client', function(done) {
      modifiedClientSchema = _.cloneDeep(clientSchema)
      delete modifiedClientSchema.properties.taxes
      const client = jsonSchemaTable('client', modifiedClientSchema, {
        db,
        datetime: true
      })
      client
          .create()
          .then(function() {
            return client.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(1)
              expect(metadata.primaryKey[0]).to.equal('clientId')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              const createdSchema = _.extend({}, modifiedClientSchema)
              createdSchema.properties = _.pick(modifiedClientSchema.properties, [
                'id',
                'initials',
                'sales',
                'debt',
                'lastSale'
              ])
              checkColumns(metadata.columns, createdSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create façade', function(done) {
      const façade = jsonSchemaTable('façade', façadeSchema, {db})
      façade
          .create()
          .then(function() {
            return façade.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(1)
              expect(metadata.primaryKey[0]).to.equal('Nome')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, façadeSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create person façade', function(done) {
      const personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {
        db
      })
      personFaçade
          .create()
          .then(function() {
            return personFaçade.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(2)
              expect(metadata.primaryKey[0]).to.equal('person')
              expect(metadata.primaryKey[1]).to.equal('XfaçadeX')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, personFaçadeSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create person tax', function(done) {
      const tax = jsonSchemaTable('tax', taxSchema, {db})
      tax
          .create()
          .then(function() {
            return tax.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(2)
              expect(metadata.primaryKey[0]).to.equal('city')
              expect(metadata.primaryKey[1]).to.equal('state')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, taxSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create catalog', function(done) {
      const catalog = jsonSchemaTable('catalog', catalogSchema, {db})
      catalog
          .create()
          .then(function() {
            return catalog.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(2)
              expect(metadata.primaryKey[0]).to.equal('catnum')
              expect(metadata.primaryKey[1]).to.equal('refnum')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, catalogSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create reffab', function(done) {
      const reffab = jsonSchemaTable('reffab', reffabSchema, {db})
      reffab
          .create()
          .then(function() {
            return reffab.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('uniqueKeys')
              expect(metadata.uniqueKeys).to.be.a('array')
              expect(metadata.uniqueKeys.length).to.equal(3)
              expect(metadata.uniqueKeys[0]).to.be.a('array')
              expect(metadata.uniqueKeys[0].length).to.equal(4)
              expect(metadata.uniqueKeys[0][0]).to.equal('NUMREF')
              expect(metadata.uniqueKeys[0][1]).to.equal('NUMCAT')
              expect(metadata.uniqueKeys[0][2]).to.equal('MODEL')
              expect(metadata.uniqueKeys[0][3]).to.equal('REF')

              expect(metadata.uniqueKeys[1]).to.be.a('array')
              expect(metadata.uniqueKeys[1].length).to.equal(1)
              expect(metadata.uniqueKeys[1][0]).to.equal('REF')

              expect(metadata.uniqueKeys[2]).to.be.a('array')
              expect(metadata.uniqueKeys[2].length).to.equal(2)
              expect(metadata.uniqueKeys[2][0]).to.equal('REF')
              expect(metadata.uniqueKeys[2][1]).to.equal('ORIGINAL')

              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, reffabSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should create refforfab', function(done) {
      const refforfab = jsonSchemaTable('refforfab', refforfabSchema, {db})
      refforfab
          .create()
          .then(function() {
            return refforfab.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(5)
              expect(metadata.primaryKey[0]).to.equal('NUMCAT')
              expect(metadata.primaryKey[1]).to.equal('NUMREF')
              expect(metadata.primaryKey[2]).to.equal('MODEL')
              expect(metadata.primaryKey[3]).to.equal('REF')
              expect(metadata.primaryKey[4]).to.equal('SUPPLIER')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, refforfabSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
  })

  describe('table with references if the reference exists', function() {
    it('should not alter person due table state not exists', function(done) {
      const person = jsonSchemaTable('person', personSchema, {db: db})
      person
          .sync()
          .then(function() {
            return person.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('lets alter tax to add a single column primary key', function(done) {
      modifiedTaxSchema = _.cloneDeep(taxSchema)
      modifiedTaxSchema.properties.id = {
        type: 'integer',
        autoIncrement: true,
        primaryKey: true
      }
      modifiedTaxSchema.required = modifiedTaxSchema.primaryKey
      delete modifiedTaxSchema.primaryKey
      const tax = jsonSchemaTable('tax', modifiedTaxSchema, {db: db})
      tax
          .sync()
          .then(function() {
            return tax.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              metadata.should.have.property('primaryKey')
              expect(metadata.primaryKey).to.be.a('array')
              expect(metadata.primaryKey.length).to.equal(1)
              expect(metadata.primaryKey[0]).to.equal('id')
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, modifiedTaxSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should add 4 foreign key to table client', function(done) {
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            return client.metadata().then(function(metadata) {
              metadata.should.have.property('foreignKeys')
              expect(metadata.foreignKeys).to.be.a('array')
              expect(metadata.foreignKeys.length).to.equal(4)
              checkForeignKey(metadata.foreignKeys, ['clientId'], 'person', [
                'personId'
              ])
              checkForeignKey(metadata.foreignKeys, ['cityTax'], 'tax', ['id'])
              checkForeignKey(metadata.foreignKeys, ['stateTax'], 'tax', ['id'])
              checkForeignKey(metadata.foreignKeys, ['tax'], 'tax', ['id'])
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should not add foreign key to table façade', function(done) {
      const façade = jsonSchemaTable('façade', façadeSchema, {db: db})
      façade
          .sync()
          .then(function() {
            return façade.metadata().then(function(metadata) {
              metadata.should.not.have.property('foreignKeys')
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should add 2 foreign keys to table personFaçade', function(done) {
      const personFaçade = jsonSchemaTable('personFaçade', personFaçadeSchema, {
        db: db
      })
      personFaçade
          .sync()
          .then(function() {
            return personFaçade.metadata().then(function(metadata) {
              metadata.should.have.property('foreignKeys')
              expect(metadata.foreignKeys).to.be.a('array')
              expect(metadata.foreignKeys.length).to.equal(2)
              checkForeignKey(metadata.foreignKeys, ['XfaçadeX'], 'façade', [
                'Nome'
              ])
              checkForeignKey(metadata.foreignKeys, ['person'], 'person', [
                'fieldName'
              ])
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
  })

  describe('modify structure', function() {
    it('should not alter person column dateOfBirth', function(done) {
      modifiedPersonSchema = _.cloneDeep(personSchema)
      modifiedPersonSchema.properties.dateOfBirth.type = 'string'
      const person = jsonSchemaTable('person', modifiedPersonSchema, {db: db})
      person
          .sync()
          .then(function() {
            done(new Error('Invalid alter table'))
          })
          .catch(function(error) {
            expect(
                error.message.indexOf('dateOfBirth cannot be modified') !== -1
            ).to.equal(true)
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should not alter client column initials to a small string', function(done) {
      modifiedClientSchema.properties.initials.maxLength = 2
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            done(new Error('Invalid alter table'))
          })
          .catch(function(error) {
            expect(
                error.message.indexOf('initials cannot be modified') !== -1
            ).to.equal(true)
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should alter client column initials to a bigger string', function(done) {
      modifiedClientSchema.properties.initials.maxLength = 21
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            return client.metadata().then(function(metadata) {
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, modifiedClientSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should not alter client column sales to a small number of decimals', function(done) {
      modifiedClientSchema.properties.sales.decimals = 4
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            done(new Error('Invalid alter table'))
          })
          .catch(function(error) {
            expect(
                error.message.indexOf('sales cannot be modified') !== -1
            ).to.equal(true)
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should not alter client column sales to a bigger number of decimals', function(done) {
      modifiedClientSchema.properties.sales.decimals = 8
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            done(new Error('Invalid alter table'))
          })
          .catch(function(error) {
            expect(
                error.message.indexOf('sales cannot be modified') !== -1
            ).to.equal(true)
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should not alter client column sales to a bigger number of decimals with no equivalent greater length', function(done) {
      modifiedClientSchema.properties.sales.maxLength = 21
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            done(new Error('Invalid alter table'))
          })
          .catch(function(error) {
            expect(
                error.message.indexOf('sales cannot be modified') !== -1
            ).to.equal(true)
            done()
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should alter client column sales to a bigger number of decimals and length', function(done) {
      modifiedClientSchema.properties.sales.maxLength = 22
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            return client.metadata().then(function(metadata) {
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, modifiedClientSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should add unique key to client', function(done) {
      modifiedClientSchema.unique = [['initials']]
      const client = jsonSchemaTable('client', modifiedClientSchema, {db: db})
      client
          .sync()
          .then(function() {
            return client.metadata().then(function(metadata) {
              metadata.should.have.property('uniqueKeys')
              expect(metadata.uniqueKeys).to.be.a('array')
              expect(metadata.uniqueKeys.length).to.equal(1)
              expect(metadata.uniqueKeys[0]).to.be.a('array')
              expect(metadata.uniqueKeys[0].length).to.equal(1)
              expect(metadata.uniqueKeys[0][0]).to.equal('initials')
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
    it('should alter person column name to text', function(done) {
      modifiedPersonSchema.properties.dateOfBirth.type = 'date'
      modifiedPersonSchema.properties.name.type = 'text'
      delete modifiedPersonSchema.properties.name.type
      const person = jsonSchemaTable('person', modifiedPersonSchema, {db: db})
      person
          .sync()
          .then(function() {
            return person.metadata().then(function(metadata) {
              metadata.should.have.property('columns')
              expect(metadata.columns).to.be.a('object')
              checkColumns(metadata.columns, modifiedPersonSchema)
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
  })

  describe('tables with multiple columns references', function() {
    it('should add 2 foreign keys to table reffab', function(done) {
      const reffab = jsonSchemaTable('reffab', reffabSchema, {db: db})
      reffab
          .sync()
          .then(function() {
            return reffab.metadata().then(function(metadata) {
              metadata.should.have.property('foreignKeys')
              expect(metadata.foreignKeys).to.be.a('array')
              expect(metadata.foreignKeys.length).to.equal(1)
              checkForeignKey(
                  metadata.foreignKeys,
                  ['NUMCAT', 'NUMREF'],
                  'catalog',
                  ['catnum', 'refnum']
              )
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })

    it('should add 4 foreign keys to table refforfab', function(done) {
      const refforfab = jsonSchemaTable('refforfab', refforfabSchema, {db: db})
      refforfab
          .sync()
          .then(function() {
            return refforfab.metadata().then(function(metadata) {
              metadata.should.have.property('foreignKeys')
              expect(metadata.foreignKeys).to.be.a('array')
              expect(metadata.foreignKeys.length).to.equal(1)
              checkForeignKey(
                  metadata.foreignKeys,
                  ['NUMREF', 'NUMCAT', 'MODEL', 'REF'],
                  'reffab',
                  ['NUMREF', 'NUMCAT', 'MODEL', 'REF']
              )
              done()
            })
          })
          .catch(function(error) {
            done(error)
          })
    })
  })
}
