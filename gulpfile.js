var path = require('path');
var gulp = require('gulp');
var eslint = require('gulp-eslint');
var mocha = require('gulp-mocha');
var istanbul = require('gulp-istanbul');
var nsp = require('gulp-nsp');
var coveralls = require('gulp-coveralls');

gulp.task('static', function() {
  return gulp.src('src/**/*.js')
    .pipe(eslint())
    .pipe(eslint.format())
    .pipe(eslint.failAfterError());
});

gulp.task('nsp', function(cb) {
  nsp('package.json', cb);
});

gulp.task('pre-test', function() {
  return gulp.src('src/**/*.js')
    .pipe(istanbul({includeUntested: true}))
    .pipe(istanbul.hookRequire());
});

gulp.task('test', ['pre-test'], function(cb) {
  var error;
  gulp.src('test/index.js')
    .pipe(mocha({reporter: 'spec', timeout: 15000}))
    .on('error', function(e) {
      error = e;
      cb(error);
    })
    .pipe(istanbul.writeReports({
      reporters: ['json', 'text', 'text-summary', 'lcov']
    }))
    .on('end', function() {
      if (!error) cb();
    });
});

gulp.task('coveralls', ['test'], function() {
  if (!process.env.CI) {
    return;
  }

  return gulp.src(path.join(__dirname, 'coverage/lcov.info'))
    .pipe(coveralls());
});

gulp.task('prepublish', ['nsp']);
gulp.task('default', ['static', 'test', 'coveralls']);
