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
var pgPromise = require('pg-promise');
var studentSchema = require('./student.json');
var classSchema = require('./class.json');

// initialize and connect to a database

var studentTable = jsonSchemaTable('person', studentSchema, {db: pgPromise});
var classTable = jsonSchemaTable('student', classSchema, {db: pgPromise});

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
 For the db connection you can use [mssql](https://github.com/patriksimek/node-mssql)
 or [pg-promise](https://github.com/vitaly-t/pg-promise)
 
 To more details take a look at the tests
 
## Tests

The tests were executed using SQL Server 2014 and Postgres 9.4.
The coverage badge don't include the SQL Server tests, the actual
coverage is 95.45%

```
--------------------|----------|----------|----------|----------|----------------|
File                |  % Stmts | % Branch |  % Funcs |  % Lines |Uncovered Lines |
--------------------|----------|----------|----------|----------|----------------|
 json-schema-table/ |    95.45 |    88.42 |      100 |    95.45 |                |
  index.js          |    95.45 |    88.42 |      100 |    95.45 |... 344,358,400 |
--------------------|----------|----------|----------|----------|----------------|
All files           |    95.45 |    88.42 |      100 |    95.45 |                |
--------------------|----------|----------|----------|----------|----------------|


=============================== Coverage summary ===============================
Statements   : 95.45% ( 252/264 )
Branches     : 88.42% ( 168/190 )
Functions    : 100% ( 51/51 )
Lines        : 95.45% ( 252/264 )
================================================================================
```


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
