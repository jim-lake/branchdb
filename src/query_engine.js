'use strict';

const _ = require('lodash');
const async = require('async');
const pg_errors = require('./pg_errors.js');

exports.runQuery = runQuery;

function runQuery(params,done) {
  const { statement, database, transaction } = params;
  const search_path = transaction.getSearchPath();

  //console.log(JSON.stringify(statement,null," "));

  let schema;
  let query_plan;
  let row_list;
  async.series([
  (done) => {
    database.getSchema(transaction,(err,s) => {
      schema = s;
      done(err);
    });
  },
  (done) => {
    const opts = {
      statement,
      schema,
      search_path,
    };
    _build_query_plan(opts,(err,plan) => {
      query_plan = plan;
      done(err);
    });
  },
  (done) => {
    const opts = {
      transaction,
      schema,
      query_plan,
    };
    _run_query_plan(opts,(err,rows) => {
      row_list = rows;
      done(err);
    });
  }],
  (err) => {
    let result = {
      cmd: 'SELECT',
      format_list: query_plan && query_plan.result_format_list,
      row_list,
    };
    done(err,result);
  });
}

function _build_query_plan(params,done) {
  try {
    const { statement, schema, search_path } = params;

    const table_alias_map = {};
    const from_table = _resolve_table(statement.from,schema,search_path);
    table_alias_map[from_table.table_alias] = from_table;

    const {
      result_format_list,
      result_column_list,
    } = _resolve_results(statement.result,table_alias_map);

    const operation_list = _build_operation_list(statement,table_alias_map,result_column_list);

    const query_plan = {
      table_alias_map,
      result_format_list,
      result_column_list,
      operation_list,
    };
    done(null,query_plan);
  } catch(e) {
    if (e.code) {
      done(e);
    } else {
      console.error("_build_query_plan: exception:",e);
      done(pg_errors.internal(e));
    }
  }
}

function _run_query_plan(params,done) {
  const { transaction, schema, query_plan } = params;
  const { result_column_list, operation_list } = query_plan;

  done(pg_errors.NOT_IMPLEMENTED_ERROR)
}


function _resolve_table(table_ref,schema,search_path) {
  const table_name = table_ref.name;
  let schema_name = table_ref.schema;
  const table_alias = table_ref.alias || table_name;

  if (!schema_name) {
    _.until(search_path,s => {
      if (table_name in schema.schema_map[s].table_map) {
        schema_name = s;
      }
      return schema_name;
    });
  }
  if (!schema_name) {
    throw pg_errors.get('UNKNOWN_TABLE_ERROR',"Undefined table: " + table_name);
  }
  if (!schema.schema_map[schema_name]) {
    throw pg_errors.get('UNKNOWN_SCHEMA_ERROR',"Undefined schema: " + schema_name);
  }
  const { table_map } = schema.schema_map[schema_name];
  const table = table_map[table_name];
  if (!table) {
    throw pg_errors.get('UNKNOWN_TABLE_ERROR',"Undefined table: " + table_name);
  }

  return {
    table_alias,
    schema_name,
    table_name,
    table,
  };
}

function _resolve_results(result,table_alias_map) {
  const result_format_list = [];
  const result_column_list = [];

  result.forEach(r => {

  });

  return {
    result_format_list,
    result_column_list,
  };
}

function _build_operation_list(statement,table_alias_map,result_column_list) {
  const operation_list = [];

  return operation_list;
}

