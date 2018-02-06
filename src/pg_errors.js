'use strict';

const _ = require('lodash');

const { SQLSTATE_ERROR_CODE_MAP } = require('node-postgres-server/constants.js');

exports.get = get;

exports.ABORT_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.in_failed_sql_transaction,
  message: "Current transaction is aborted, commands ignored until end of transaction block",
};

exports.SYNTAX_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.syntax_error,
  message: "Syntax error",
};
exports.UNKNOWN_CMD_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.syntax_error,
  message: "Unknown command",
};
exports.INVALID_TRANSACTION_STATE_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.invalid_transaction_state,
  message: "Invalid transaction state",
};
exports.INVALID_DB_NAME_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.reserved_name,
  message: "Invalid database name",
};
exports.DUP_DB_NAME_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.duplicate_database,
  message: "Database name already exists",
};

function get(error_name,extra) {
  const new_error = _.extend({},exports[error_name] || exports.SYNTAX_ERROR);
  if (typeof extra == 'string') {
    new_error.message = extra;
  } else if (typeof extra == 'object') {
    _.extend(new_error,extra);
  }
  return new_error;
}
