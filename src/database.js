'use strict';

const async = require('async');
const pg_errors = require('./pg_errors.js');
const Commit = require('./commit.js');

module.exports = Database;

function Database(opts) {
  if (this instanceof Database) {
    this._name = opts.name;
    this._is_internal = !!opts.is_internal;
  } else {
    return new Database(opts);
  }
}
Database.prototype.getName = function() {
  return this._name;
};

Database.prototype.createSchema = function(opts,done) {
  const { name, transaction, } = opts;
  this.getSchema(transaction,(err,schema) => {
    if (!err) {
      if (schema.schema_map[name]) {
        err = pg_errors.DUP_SCHEMA_NAME_ERROR;
      } else {
        const cmd = 'CREATE_SCHEMA';
        const log = "CREATE SCHEMA $1i";
        const args = [name];
        const schema_update = { cmd, name };
        const op = { cmd, log, args, schema_update };
        transaction.addOperation(op);
      }
    }
    done(err);
  });
};

Database.prototype.dropSchema = function(opts,done) {
  const { name, transaction, } = opts;
  this.getSchema(transaction,(err,schema) => {
    if (!err) {
      if (!schema.schema_map[name]) {
        err = pg_errors.SCHEMA_DOES_NOT_EXIST_ERROR;
      } else {
        const cmd = 'DROP_SCHEMA';
        const log = "DROP SCHEMA $1i";
        const args = [name];
        const schema_update = { cmd, name };
        const op = { cmd, log, args, schema_update };
        transaction.addOperation(op);
      }
    }
    done(err);
  });
};

Database.prototype.createTable = function(opts,done) {
  done(pg_errors.NOT_IMPLEMENTED_ERROR);
};

Database.prototype.getCommitByString = function(s,done) {
  if (this._is_internal) {
    done('not_found');
  } else {
    const info = Commit.stringToCommitInfo(s)
    if (!info) {
      done('invalid_id');
    } else if (info.commit_id == 0) {
      done(null,new Commit(0));
    } else {
      const { commit_id } = info;

      const opts = {
        database: this._name,
        commit_id,
      };
      data_store.findCommit(opts,done);
    }
  }
};

Database.prototype.getSchema = function(transaction,done) {
  const commit = transaction.getCommit();
  const schema = {
    schema_map: {},
    table_map: {},
  };

  done(null,schema);
};

Database.prototype.getNextCommit = function(opts,done) {
  const { client, commit, commit_hash } = opts;

  done(null,commit.getNextCommit(commit_hash));
};
