'use strict';

const async = require('async');
const pg_errors = require('./pg_errors.js');
const parent_db = require('../lib/db.js');

module.exports = Transaction;

function Transaction(connection) {
  if (this instanceof Transaction) {
    this._connection = connection;
    this._command_count = 0;
    this._is_complete = false;
    this._is_aborted = false;
    this._is_implicit_abort = false;
    this._branch_mode = "branch";
    this._is_auto_commit = connection.isAutoCommit();
    this._search_path = connection.getSearchPath();
    this._commit = connection.getCommit();
    this._operation_list = [];
  } else {
    return new Transaction(connection);
  }
}

Transaction.prototype.getCommandCount = function() {
  return this._command_count;
};
Transaction.prototype.isAborted = function() {
  return this._is_aborted;
};
Transaction.prototype.isAutoCommit = function() {
  return this._is_auto_commit;
};
Transaction.prototype.isComplete = function() {
  return this._is_complete;
};
Transaction.prototype.getSearchPath = function() {
  return this._search_path;
};
Transaction.prototype.setSearchPath = function(path) {
  this._search_path = path;
};
Transaction.prototype.getCommit = function() {
  return this._commit;
};

Transaction.prototype.abort = function() {
  this._is_aborted = true;
};

// Used for create and drop database that can't happen in a transaction
Transaction.prototype.implicitAbort = function() {
  this._is_aborted = true;
  this._is_implicit_abort = true;
  if (this._is_auto_commit) {
    // In a auto commit transaction, so just complete it and turn off auto commit
    this._is_complete = true;
    this._is_auto_commit = false;
  } else {
    // We're in a transaction we're going to abort
  }
};

Transaction.prototype.begin = function(done) {
  this._is_auto_commit = false;
  done();
};

Transaction.prototype.commit = function(done) {
  if (this._is_aborted) {
    this._is_complete = true;
    done(null,"ROLLBACK");
  } else if (this._is_complete) {
    console.error("Transaction.commit: Double commit transaction, shouldn't be possible");
    done(pg_errors.INVALID_TRANSACTION_STATE_ERROR);
  } else if (this._operation_list.length == 0) {
    this._is_complete = true;
    done(null,"COMMIT");
  } else {
    let client = null;
    let next_commit = null;
    const sql_list = [];
    const database = this._connection.getDatabase();
    const db_name = database.getName();
    const user_name = this._connection.getUserName();

    const commit_log = this._operation_list.map(op => {
      const { log, args } = op;
      return parent_db.preparse(log,args);
    }).join(";");
    const commit_hash = this._commit.nextHash(commit_log);

    async.series([
    done => {
      const sql = parent_db.preparse("BEGIN;SET LOCAL search_path = $1i;",[db_name]);
      parent_db.queryWithClient(sql,(err,res,c) => {
        if (err) {
          console.error("Transaction.commit: begin err:",err);
        }
        client = c;
        done(err);
      });
    },
    done => {
      const opts = {
        client,
        commit: this._commit,
        branch_mode: this._branch_mode,
        commit_hash,
      };
      database.getNextCommit(opts,(err,next) => {
        next_commit = next;
        done(err);
      });
    },
    done => {
      let err = null;
      const next_commit_id = next_commit.toIntString();
      this._operation_list.forEach(op => {
        const { sql, log, args } = op;
        args.push(next_commit_id);
        const new_sql = parent_db.preparse(sql,args);
        sql_list.push(new_sql);
      });

      if (err) {
        done(err);
      } else {
        const value = {
          commit_id: next_commit_id,
          commit_hash,
          user_name,
          commit_log,
        };
        const sql = parent_db.preparse("INSERT INTO commit $1v",[value]);
        client.query(sql,(err,res) => {
          if (err) {
            console.error("Transaction.commit: create commit err:",err);
          }
          done(err);
        });
      }
    },
    done => {
      const sql = sql_list.join(';');
      client.query(sql,(err) => {
        if (err) {
          console.error("Transaction.commit: execute sql:",sql,",err:",err);
        }
        done(err);
      });
    },
    done => {
      parent_db.commit(client,(err) => {
        if (err) {
          console.error("Transaction.commit: commit err:",err);
        }
        done(err);
      });
    }],
    err => {
      this._is_complete = true;
      this._is_auto_commit = false;
      if (err) {
        this._is_aborted = true;
        parent_db.rollback(client);
        err = pg_errors.internal(err);
      } else {
        this._connection.setCommit(next_commit);
      }
      done(err,"COMMIT",next_commit && next_commit.toHexString());
    });
  }
};
Transaction.prototype.rollback = function(done) {
  this._is_aborted = true;
  this._is_complete = true;
  done(null,"ROLLBACK");
};
Transaction.prototype.addOperation = function(op) {
  this._operation_list.push(op);
};
