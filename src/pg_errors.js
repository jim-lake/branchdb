'use strict';

const _ = require('lodash');

const { SQLSTATE_ERROR_CODE_MAP } = require('node-postgres-server/constants.js');

exports.get = get;
exports.internal = internal;

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
exports.DUP_SCHEMA_NAME_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.duplicate_schema,
  message: "Schema name already exists",
};
exports.DB_DOES_NOT_EXIST_ERROR = {
  severity: 'FATAL',
  code: SQLSTATE_ERROR_CODE_MAP.invalid_catalog_name,
  message: "Database does not exist",
};
exports.SCHEMA_DOES_NOT_EXIST_ERROR = {
  severity: 'FATAL',
  code: SQLSTATE_ERROR_CODE_MAP.invalid_schema_name,
  message: "Schema does not exist",
};
exports.INTERNAL_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.internal_error,
  message: "Internal error",
};
exports.NOT_IMPLEMENTED_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.internal_error,
  message: "Not implemented",
};
exports.UNKNOWN_VARIABLE_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.invalid_name,
  message: "Unrecognized configuration parameter",
};
exports.UNKNOWN_COMMIT_ID_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.internal_error,
  message: "Unknown transaction ID",
};
exports.COMMIT_CONFLICT_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.exclusion_violation,
  message: "Transaction conflict",
};
exports.UNKNOWN_TABLE_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.undefined_table,
  message: "Undefined table",
};
exports.UNKNOWN_SCHEMA_ERROR = {
  severity: 'ERROR',
  code: SQLSTATE_ERROR_CODE_MAP.invalid_schema_name,
  message: "Undefined schema",
};

function internal(err) {
  const new_error = _.extend({},exports.INTERNAL_ERROR);

  new_error.message = err;
  if (err.stack) {
    new_error.message = err.stack;
  }
  return new_error;
}

function get(error_name,extra) {
  const new_error = _.extend({},exports[error_name] || exports.SYNTAX_ERROR);
  if (typeof extra == 'string') {
    new_error.message = extra;
  } else if (typeof extra == 'object') {
    _.extend(new_error,extra);
  }
  return new_error;
}
