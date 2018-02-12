'use strict';

const async = require('async');
const parser = require('./parser.js');
const Transaction = require('./transaction.js');
const Commit = require('./commit.js');
const data_store = require('./data_store.js');
const pg_errors = require('./pg_errors.js');

module.exports = Connection;

function Connection(pg_client) {
  if (this instanceof Connection) {
    this._pg_client = pg_client;
    this._transaction = false;
    this._is_auto_commit = true;
    this._database = false;
    this._search_path = ['public'];
    this._commit = new Commit(0);
    this._user_name = "";
    this._addListeners();
  } else {
    return new Connection(pg_client);
  }
}
Connection.prototype.isAutoCommit = function() {
  return this._is_auto_commit;
};
Connection.prototype.getSearchPath = function() {
  return this._search_path;
};
Connection.prototype.getCommit = function() {
  return this._commit;
};
Connection.prototype.getDatabase = function() {
  return this._database;
};
Connection.prototype.getUserName = function() {
  return this._user_name;
};
Connection.prototype.setCommit = function(commit) {
  this._commit = commit;
};

Connection.prototype._onQuery = function(query) {
  try {
    const ast = parser.parse(query);

    async.eachSeries(ast.statement,(s,done) => {
      this._transactStatement(s,(err,result) => {
        if (err) {
          console.error("Connection._onQuery: transact error:",err);
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
    let err;
    if (e instanceof parser.SyntaxError) {
      console.error("Connection._onQuery: syntax error for query:",query);
      err = pg_errors.get('SYNTAX_ERROR',"Syntax error: " + e);
    } else {
      console.error("Connection._onQuery: internal error:",e.stack);
      err = pg_errors.internal(e);
    }
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
    database: this._database,
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
      this._transaction.commit((err,result,commit_id) => {
        done(err,{ cmd: result, oid: commit_id });
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
    case "variable:show": {
      const name = statement.target;
      this._showVariable(name,done);
      break;
    }
    case "variable:set": {
      const name = statement.target;
      const value = statement.value;
      this._setVariable(name,value,done);
      break;
    }
    case "create:schema": {
      params.name = statement.target.name;
      this._database.createSchema(params,err => {
        done(err,{ cmd: "CREATE SCHEMA" });
      });
      break;
    }
    case "drop:schema": {
      params.name = statement.target.name;
      this._database.dropSchema(params,err => {
        done(err,{ cmd: "DROP SCHEMA" });
      });
      break;
    }
    case "create:table": {
      params.name = statement.target.name;
      this._database.createTable(params,err => {
        done(err,{ cmd: "CREATE TABLE" });
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

const DB_SPEC_REGEX = /^([^:]*):?(.*)?$/;

Connection.prototype._onConnect = function(params) {
  const { database, user } = params;
  this._user_name = user;

  const match = database.match(DB_SPEC_REGEX);
  if (!match || match.length < 2) {
    this._pg_client.sendErrorResponse(pg_errors.INVALID_DB_NAME_ERROR);
  } else {
    const db_name = match[1];
    const rev = match[2] || 'master';

    let db = false;
    let commit = false;
    async.series([
    (done) => {
      data_store.findDatabase(db_name,(err,found_db) => {
        db = found_db;
        done(err);
      });
    },
    (done) => {
      if (db.isInternal()) {
        commit = new Commit(0);
        done();
      } else {
        db.getCommitByString(rev,(err,found_commit) => {
          commit = found_commit;
          done(err);
        });
      }
    }],
    (err) => {
      if (err) {
        let e;
        if (err == 'illegal_name') {
          e = pg_errors.INVALID_DB_NAME_ERROR;
        } else if (err == 'db_not_found') {
          e = pg_errors.DB_DOES_NOT_EXIST_ERROR;
        } else if (err == 'commit_not_found' || err == 'label_not_found' || err == 'hash_not_found') {
          e = pg_errors.UNKNOWN_COMMIT_ID;
        } else {
          e = pg_errors.internal(err);
        }
        this._pg_client.sendErrorResponse(e);
      } else {
        this._database = db;
        this._commit = commit;
        this._pg_client.sendAuthenticationCleartextPassword();
      }
    });
  }
};

Connection.prototype._showVariable = function(name,done) {
  let err = null;
  let value = "";
  switch(name) {
    case 'schema':
    case 'search_path': {
      value = this._transaction.getSearchPath().join(',');
      break;
    }
    case 'current_commit':
      value = this._commit.toHexString();
      break;
    default:
      err = pg_errors.UNKNOWN_VARIABLE_ERROR;
      break;
  };
  let result = {
    cmd: 'SHOW',
    format_list: [{ name, type: 'text' }],
    row_list: [[value]],
  };
  done(err,result);
};
Connection.prototype._setVariable = function(name,value,done) {
  switch(name) {
    case 'schema':
    case 'search_path': {
      const path = value.split(',').map(s => s.trim());
      this._transaction.setSearchPath(path);
      this._search_path = path;
      done(null,{ cmd: "SET" });
      break;
    }
    case 'current_commit': {
      this._database.getCommitByString(value,(err,commit) => {
        if (err == 'not_found' || err == 'invalid_id') {
          err = pg_errors.UNKNOWN_COMMIT_ID;
        } else if (err) {
          err = pg_errors.internal(err);
        } else {
          this._commit = commit;
        }
        done(err,{ cmd: "SET" });
      });
      break;
    }
    default:
      done(pg_errors.UNKNOWN_VARIABLE_ERROR);
      break;
  };
};

Connection.prototype._addListeners = function() {
 this._pg_client.on('connect',this._onConnect.bind(this));
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
