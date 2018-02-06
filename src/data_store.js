'use strict';

const pg_errors = require('./pg_errors.js');

exports.createDatabase = createDatabase;
exports.dropDatabase = dropDatabase;
exports.findDatabase = findDatabase;

function is_reserved_database_name(name) {
  let ret = false;
  if (name == 'branchdb' || name == 'postgres') {
    ret = true;
  }
  return ret;
}

function createDatabase(opts,done) {
  const { name } = opts;
  if (is_reserved_database_name(name)) {
    done(pg_errors.INVALID_DB_NAME_ERROR)
  } else {
    console.log("createDatabase:",name);
    done();
  }
}

function dropDatabase(opts,done) {
  const { name } = opts;
  if (is_reserved_database_name(name)) {
    done(pg_errors.INVALID_DB_NAME_ERROR)
  } else {
    console.log("dropDatabase:",name)
    done();
  }
}

function findDatabase() {

}
