'use strict';

const _ = require('lodash');
const config = require('../config.json');
const async = require('async');
const pg_escape = require('pg-escape');
const { Pool } = require('pg');

const pool = new Pool(config.db);

exports._pool = pool;
exports.query = query;
exports.queryMulti = queryMulti;
exports.queryPreparse = queryPreparse;
exports.queryWithClient = queryWithClient;
exports.release = release;
exports.commit = commit;
exports.rollback = rollback;
exports.preparse = preparse;

pool.on('error', (err, client) => {
  console.error("lib/db: Unexpected error on idle client:",err);
});

function query(sql,args,done) {
  if (typeof args == 'function') {
    done = args;
    args = [];
  }

  pool.query(sql,args,(err,res) => {
    done(err,res);
  });
}
function queryMulti(sql,args,done) {
  if (typeof args == 'function') {
    done = args;
    args = [];
  }
  const sql_list = sql.split(';');

  pool.connect((err,client,client_release) => {
    let res_list = [];
    async.eachSeries(sql_list,(sql,done) => {
      const t = _pick_args(sql,args);
      client.query(t.sql,t.args,(err,res) => {
        res_list.push(res);
        done(err);
      });
    },(err) => {
      client_release();
      done(err,res_list);
    });
  });
}
function queryPreparse(sql,args,done) {
  const new_sql = preparse(sql,args);
  pool.query(new_sql,done);
}

function queryWithClient(sql,args,done) {
  if (typeof args == 'function') {
    done = args;
    args = [];
  }
  pool.connect((err,client,client_release) => {
    if (err) {
      done(err);
    } else {
      client.query(sql,args,(err,res) => {
        done(err,res,client);
      });
    }
  });
}

function release(client,err) {
  if (client) {
    client.query("ROLLBACK",(err,res) => {
      client.release(err);
    });
  }
}

function commit(client,done) {
  if (!done) {
    done = function() {};
  }
  client.query("COMMIT",(err,res) => {
    client.release();
    done(err);
  });
}
function rollback(client,done) {
  if (!done) {
    done = function() {};
  }
  if (client) {
    client.query("ROLLBACK",(err,res) => {
      client.release();
      done(err);
    });
  } else {
    setImmediate(done);
  }
}

function preparse(sql,args) {
  const new_sql = sql.replace(/\$(\d*)([ilv]?)/g,(match,index,type) => {
    let ret = "";
    const val = args[index - 1];
    try {
      if (type == 'i') {
        ret = pg_escape.ident(val);
      } else if (type == 'l') {
        ret = pg_escape.literal(val);
      } else if (type == 'v') {
        const names = [];
        const values = [];
        _.each(val,(v,k) => {
          names.push(pg_escape.ident(k));
          if (Buffer.isBuffer(v)) {
            const base64 = v.toString('base64');
            values.push("decode('" + base64  +"','base64')");
          } else {
            values.push(pg_escape.literal(v));
          }
        });
        ret = "(" + names.join(',') + ") VALUES (" + values.join(",") + ")";
      } else {
        ret = pg_escape.string(val);
      }
    } catch(e) {
      console.error("db.preparse: exception:",e.stack,{ val });
    }
    return ret;
  });
  return new_sql;
}

function _pick_args(sql,args) {
  let ret_args = [];
  let arg_map = {};
  const ret_sql = sql.replace(/\$(\d*)/g,(_ignore,index) => {
    let ret;
    if (index in arg_map) {
      ret = arg_map[index];
    } else {
      const new_index = ret_args.length + 1;
      arg_map[index] = new_index;
      ret = ret_args.push(args[index - 1]);
    }

    return "$" + ret;
  });
  return { sql: ret_sql, args: ret_args };
}

if (require.main === module) {
  console.log(_pick_args("SELECT $2,$3,$1",[1,2,3,4]));
  console.log(_pre_parse("SELECT $1l AS foo FROM $1i;SELECT $2 AS bar FROM $2i",["arg 1","arg 2"]));
}
