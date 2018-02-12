'use strict';

const async = require('async');
const pg_errors = require('./pg_errors.js');
const Commit = require('./commit.js');
const data_store = require('./data_store.js');

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
Database.prototype.isInternal = function() {
  return this._is_internal;
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

const BRANCH_COMMIT_REGEX = /^(\d*)::(\d*)$/;
const LABEL_NAME_REGEX = /^[A-Za-z0-9_-]*$/;
const COMMIT_HASH_REGEX = /^[0-9A-Fa-f]*$/;

Database.prototype.getCommitByString = function(s,done) {
  if (this._is_internal) {
    done('commit_not_found');
  } else {
    const is_branch_commit = s.match(BRANCH_COMMIT_REGEX) != null;
    const maybe_label = s.match(LABEL_NAME_REGEX) != null;
    const maybe_hash = s.match(COMMIT_HASH_REGEX) != null;

    if (is_branch_commit) {
      this._getCommitByBranchCommit(s,done);
    } else if (maybe_label) {
      this._getCommitByLabel(s,(err,commit) => {
        if (err == 'label_not_found' && maybe_hash) {
          this._getCommitByHash(s,done);
        } else {
          done(err,commit);
        }
      });
    } else {
      done('commit_not_found');
    }
  }
};

Database.prototype._getCommitByLabel = function(label,done) {
  const opts = { database: this._name, label };
  data_store.findCommitByLabel(opts,done);
};
Database.prototype._getCommitByHash = function(hash,done) {
  const opts = { database: this._name, hash };
  data_store.findCommitByHash(opts,done);
};
Database.prototype._getCommitByBranchCommit = function(s,done) {
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
