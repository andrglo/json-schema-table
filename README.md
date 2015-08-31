# json-schema-table [![NPM version][npm-image]][npm-url] [![Build Status][travis-image]][travis-url] [![Dependency Status][daviddm-image]][daviddm-url] [![Coverage percentage][coveralls-image]][coveralls-url]
> Creates and maintains a SQL table structure equivalent to a 
json schema definition. For MSSQL and Postgres

First you create all your tables and then syncronizes then 
with each other to create the references. You can sync whenever
you modify your json schema

## Install

```sh
$ npm install --save json-schema-table
```

## Usage

```js
var jsonSchemaTable = require('json-schema-table');
var pg = require('pg-cr-layer');
var studentSchema = require('./student.json');
var classSchema = require('./class.json');

// initialize and connect to a database

var studentTable = jsonSchemaTable('person', studentSchema, {db: pg});
var classTable = jsonSchemaTable('student', classSchema, {db: pg});

// First create then sync to build the references
studentTable.create().then(function() {
	return classTable.create();
}).then(function() {
	return studentTable.sync();
}).then(function() {
	return classTable.sync();
}).catch(function(error) {
	console.log(error);
});
```
 For the db connection you can use [mssql-cr-layer](https://github.com/andrglo/mssql-cr-layer)
 or [pg-cr-layer](https://github.com/andrglo/pg-cr-layer)
 
 To more details take a look at the tests
 
## License

MIT © [Andre Gloria](andrglo.com)


[npm-image]: https://badge.fury.io/js/json-schema-table.svg
[npm-url]: https://npmjs.org/package/json-schema-table
[travis-image]: https://travis-ci.org/andrglo/json-schema-table.svg?branch=master
[travis-url]: https://travis-ci.org/andrglo/json-schema-table
[daviddm-image]: https://david-dm.org/andrglo/json-schema-table.svg?theme=shields.io
[daviddm-url]: https://david-dm.org/andrglo/json-schema-table
[coveralls-image]: https://coveralls.io/repos/andrglo/json-schema-table/badge.svg?branch=master&service=github
[coveralls-url]: https://coveralls.io/github/andrglo/json-schema-table?branch=master
