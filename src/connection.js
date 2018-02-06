'use strict';

const async = require('async');
const parser = require('./parser.js');
const Transaction = require('./transaction.js');
const data_store = require('./data_store.js');
const pg_errors = require('./pg_errors.js');

module.exports = Connection;

function Connection(pg_client) {
  if (this instanceof Connection) {
    this._pg_client = pg_client;
    this._transaction = false;
    this._is_auto_commit = true;
    this._addListeners();
  } else {
    return new Connection(pg_client);
  }
}
Connection.prototype.isAutoCommit = function() {
  return this._is_auto_commit;
};

Connection.prototype._onQuery = function(query) {
  try {
    const ast = parser.parse(query);

    async.eachSeries(ast.statement,(s,done) => {
      this._transactStatement(s,(err,result) => {
        if (err) {
          this._pg_client.sendErrorResponse(err);
        } else {
          const { format_list, row_list, } = result;
          const cmd = result.cmd || "SELECT";
          const oid = result.oid || null;
          const count = (row_list && row_list.length) || "";

          if (format_list) {
            this._pg_client.sendRowDescription(format_list);
            if (row_list) {
              this._pg_client.sendDataRowList(row_list,format_list);
            }
          }
          this._pg_client.sendCommandComplete(cmd,oid,count);
        }
        done();
      });
    },
    (err) => {
      this._pg_client.sendReadyForQuery();
    });
  } catch(e) {
    if (this._transaction) {
      this._transaction.abort();
    }

    const err = pg_errors.get('SYNTAX_ERROR',"Syntax error: " + e);
    this._pg_client.sendErrorResponse(err);
    this._pg_client.sendReadyForQuery();
  }
};
Connection.prototype._transactStatement = function(statement,done) {
  if (!this._transaction) {
    this._transaction = new Transaction(this);
  }

  if (this._transaction.isAborted()) {
    // special case aborted transaction
    const statement_name = get_statement_name(statement);

    if (statement_name == 'transaction:commit' || statement_name== 'transaction:rollback') {
      this._transaction.rollback(err => {
        this._transaction = null;
        done(err,{ cmd: "ROLLBACK" });
      });
    } else {
      setImmediate(() => done(pg_errors.ABORT_ERROR,{}));
    }
  } else {
    let sql_err = false;
    let sql_result = false;

    async.series([
    (done) => {
      this._executeStatement(statement,(err,result) => {
        sql_result = result;
        sql_err = err;
        if (sql_err) {
          this._transaction.abort();
        }
        done();
      })
    },
    (done) => {
      if (this._transaction.isAutoCommit()) {
        if (sql_err) {
          this._transaction.rollback(done);
        } else {
          this._transaction.commit(err => {
            if (err) {
              sql_err = err;
              sql_result = {};
            }
            done();
          });
        }
      } else {
        done();
      }
    }],
    (err) => {
      if (this._transaction.isComplete()) {
        this._transaction = null;
      }
      done(sql_err,sql_result);
    });
  }
};
Connection.prototype._executeStatement = function(statement,done) {
  const { variant, format, action } = statement;

  const statement_name = get_statement_name(statement);
  const params = {
    client: this,
    transaction: this._transaction,
    statement,
  };

  switch(statement_name) {
    case "transaction:begin": {
      this._transaction.begin(err => {
        done(err,{ cmd: "BEGIN" });
      })
      break;
    }
    case "transaction:rollback": {
      this._transaction.rollback(err => {
        done(err,{ cmd: "ROLLBACK" });
      });
      break;
    }
    case "transaction:commit": {
      this._transaction.commit((err,result) => {
        done(err,{ cmd: result });
      });
      break;
    }
    case "create:database": {
      this._transaction.implicitAbort();
      params.name = statement.target.name;
      data_store.createDatabase(params,err => {
        done(err,{ cmd: "CREATE DATABASE" })
      });
      break;
    }
    case "drop:database": {
      this._transaction.implicitAbort();
      params.name = statement.target.name;
      data_store.dropDatabase(params,err => {
        done(err,{ cmd: "DROP DATABASE" });
      });
      break;
    }
    default: {
      console.error("Unhandled statememt:",JSON.stringify(statement,null,"  "));
      const err = pg_errors.get('UNKNOWN_COMMAND',"Unknown command: " + statement_name);
      done(err);
      break;
    }
  }
};

Connection.prototype._addListeners = function() {
 this._pg_client.on('connect',params => {
    //console.log("client_connect:",params);
    this._pg_client.sendAuthenticationCleartextPassword();
  });
  this._pg_client.on('password',password => {
    //console.log("client_password:",password);
    this._pg_client.sendAuthenticationOk();
    this._pg_client.sendReadyForQuery();
  });
  this._pg_client.on('query',this._onQuery.bind(this));
  this._pg_client.on('parse',statement => {
    console.log("client_parse:",statement);
  });
  this._pg_client.on('bind',statement => {
    console.log("client_bind:",statement);
  });
  this._pg_client.on('describe_statement',statement => {
    console.log("client_describe_statement:",statement);
  });
  this._pg_client.on('describe_portal',statement => {
    console.log("client_describe_portal:",statement);
    const format_list = [
      { name: "message", type: 'text' },
    ];
    this._pg_client.sendRowDescription(format_list);
  });
  this._pg_client.on('execute',(statement,max_rows) => {
    console.log("client_execute:",statement, { max_rows });
    const rows = [["foo"]];
    const format_list = [
      { name: "message", type: 'text' },
    ];

    this._pg_client.sendDataRowList(rows,format_list);
    this._pg_client.sendCommandComplete("SELECT",null,1);
  });
  this._pg_client.on('flush',() => {
    console.log("client_flush");
  });
  this._pg_client.on('sync',() => {
    console.log("client_sync");
    this._pg_client.sendReadyForQuery();
  });
  this._pg_client.on('terminate',() => {
    console.log("client_terminate");
    this._pg_client.end();
  });
  this._pg_client.on('socket_connect',client => {
    console.log("socket_connect");
  });
  this._pg_client.on('socket_close',client => {
    console.log("socket_close");
  });
  this._pg_client.on('socket_error',(client,err) => {
    console.log("socket_error:",err);
  });
  this._pg_client.on('socket_end',client => {
    console.log("socket_end");
  });
};

function get_statement_name(statement) {
  const { variant, format, action } = statement;
  let ret = variant;
  if (format) {
    ret += ":" + format;
  }
  if (action) {
    ret += ":" + action;
  }
  return ret;
}
