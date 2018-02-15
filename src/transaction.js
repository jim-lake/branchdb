'use strict';

const async = require('async');
const pg_errors = require('./pg_errors.js');
const data_store = require('./data_store.js');

module.exports = Transaction;

function Transaction(connection) {
  if (this instanceof Transaction) {
    this._connection = connection;
    this._command_count = 0;
    this._is_complete = false;
    this._is_aborted = false;
    this._is_implicit_abort = false;
    this._operation_list = [];

    this._branch_mode = connection.getBranchMode();
    this._tracking_mode = connection.getTrackingMode();
    this._is_auto_commit = connection.isAutoCommit();
    this._search_path = connection.getSearchPath();
    this._commit = connection.getCommit();

    if (this._tracking_mode == 'branch') {
      this._commit = connection.getDatabase().getBranchHead(this._commit);
    }
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
Transaction.prototype.getOperationList = function() {
  return this._operation_list;
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
    const opts = {
      connection: this._connection,
      database: this._connection.getDatabase(),
      operation_list: this._operation_list,
      base_commit: this._commit,
      branch_mode: this._branch_mode,
    };
    data_store.createCommit(opts,(err,next_commit) => {
      this._is_complete = true;
      this._is_auto_commit = false;
      if (err) {
        this._is_aborted = true;
        if (err == 'conflict') {
          err = pg_errors.COMMIT_CONFLICT_ERROR;
        } else {
          err = pg_errors.internal(err);
        }
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
