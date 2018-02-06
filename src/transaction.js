'use strict';

const pg_errors = require('./pg_errors.js');

module.exports = Transaction;

function Transaction(connection) {
  if (this instanceof Transaction) {
    this._connection = connection;
    this._command_count = 0;
    this._is_complete = false;
    this._is_aborted = false;
    this._is_implicit_abort = false;
    this._is_auto_commit = connection.isAutoCommit();
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
  } else {
    this._is_auto_commit = false;
    this._is_complete = true;
    done(null,"COMMIT");
  }
};
Transaction.prototype.rollback = function(done) {
  this._is_aborted = true;
  this._is_complete = true;
  done(null,"ROLLBACK");
};

