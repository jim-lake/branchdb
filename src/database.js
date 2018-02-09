'use strict';

const async = require('async');
const parent_db = require('../lib/db.js');
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
        const sql = "INSERT INTO schema (commit_id,schema_name) VALUES ($2l,$1l)";
        const log = "CREATE SCHEMA $1i";
        const args = [name];
        const schema_update = { cmd: 'CREATE_SCHEMA', name };
        const op = { sql, log, args, schema_update };
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
        const sql = "INSERT INTO schema (commit_id,is_deleted,schema_name) VALUES ($2l,TRUE,$1l)";
        const log = "DROP SCHEMA $1i";
        const args = [name];
        const schema_update = { cmd: 'DROP_SCHEMA', name };
        const op = { sql, log, args, schema_update };
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
      const sql = "SELECT * FROM $1i.commit WHERE commit_id = $2l";
      parent_db.queryPreparse(sql,[this._name,commit_id],(err,res) => {
        let commit;
        if (err) {
          console.error("Database.getCommitByString: sql err:",err);
        } else if (res.rows.length == 0) {
          err = 'not_found';
        } else {
          const row0 = res.rows[0];
          console.log("commit:",row0);
          commit = new Commit(row0);
        }
        done(err,commit);
      });
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
