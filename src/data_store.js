'use strict';

const parent_db = require('../lib/db.js');
const pg_errors = require('./pg_errors.js');
const uuidv1 = require('uuid/v1');
const Database = require('./database.js');

exports.createDatabase = createDatabase;
exports.dropDatabase = dropDatabase;
exports.findDatabase = findDatabase;

const DB_NAME_REGEX = /^[\w-]*$/;

function _is_internal_database_name(name) {
  let ret = false;
  if (name == 'branchdb' || name == 'postgres') {
    ret = true;
  }
  return ret;
}
function _is_reserved_database_name(name) {
  let ret = false;
  if (_is_internal_database_name(name) || name == 'public') {
    ret = true;
  }
  return ret;
}
function _is_valid_db_name(name) {
  let ret = true;
  if (_is_reserved_database_name(name)) {
    ret = false;
  } else if (!DB_NAME_REGEX.test(name)) {
    ret = false;
  }
  return ret;
}

function createDatabase(opts,done) {
  const { name } = opts;
  if (!_is_valid_db_name(name)) {
    done(pg_errors.INVALID_DB_NAME_ERROR)
  } else {
    const sql =
`BEGIN;
CREATE SCHEMA $1i;
CREATE TABLE $1i.commit
  (
    commit_id BIGINT PRIMARY KEY NOT NULL,
    commit_hash BYTEA NOT NULL,
    user_name VARCHAR(256) NOT NULL,
    commit_log TEXT NOT NULL
  );
CREATE INDEX commit_commit_hash_index ON $1i.commit(commit_hash);
CREATE TABLE $1i.branch
  (
    branch_number BIGINT PRIMARY KEY NOT NULL,
    parent_commit_id BIGINT NOT NULL
  );
 CREATE TABLE $1i.schema
  (
    commit_id BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    schema_name VARCHAR(256) NOT NULL
  );
 CREATE TABLE $1i.table
  (
    commit_id BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL
  );
 CREATE TABLE $1i.column
  (
    commit_id BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    column_name VARCHAR(256) NOT NULL,
    column_uuid UUID NOT NULL,
    pg_type INT NOT NULL,
    has_default BOOL DEFAULT FALSE NOT NULL,
    default_value TEXT NULL DEFAULT NULL
  );
COMMIT;
`
    parent_db.queryPreparse(sql,[name],(err,res) => {
      if (err && err.code == '42P06') {
        done(pg_errors.DUP_DB_NAME_ERROR);
      } else if (err) {
        console.error("createDatabase: Query failed:",err);
        done(pg_errors.internal(err));
      } else {
        done();
      }
    });
  }
}

function dropDatabase(opts,done) {
  const { name } = opts;
  if (_is_reserved_database_name(name)) {
    done(pg_errors.INVALID_DB_NAME_ERROR)
  } else {
    const sql = `DROP SCHEMA $1i CASCADE;`;
    parent_db.queryPreparse(sql,[name],(err,res) => {
      if (err && err.code == '3F000') {
        done(pg_errors.DB_DOES_NOT_EXIST_ERROR);
      } else if (err) {
        console.error("dropDatabase: Query failed:",err);
        done(pg_errors.internal(err));
      } else {
        done();
      }
    });
  }
}

function findDatabase(name,done) {
  if (_is_internal_database_name(name)) {
    const database = new Database({ name, is_internal: true });
    setImmediate(() => done(null,database));
  } else if (!_is_valid_db_name(name)) {
    setImmediate(() => done('invalid_name'));
  } else {
    const sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1";
    parent_db.query(sql,[name],(err,res) => {
      let database = false;
      if (err) {
        console.error("");
      } else if (res.rows.length == 0) {
        err = 'not_found';
      } else {
        database = new Database({ name });
      }
      done(err,database);
    });
  }
}
