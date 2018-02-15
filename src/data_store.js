'use strict';

const _ = require('lodash');
const async = require('async');
const parent_db = require('../lib/db.js');
const pg_errors = require('./pg_errors.js');
const uuidv1 = require('uuid/v1');
const Database = require('./database.js');
const Commit = require('./commit.js');

const {
  DB_NAME_REGEX,
} = require('./constants.js');

exports.createDatabase = createDatabase;
exports.dropDatabase = dropDatabase;
exports.findDatabase = findDatabase;
exports.createCommit = createCommit;
exports.findCommit = findCommit;
exports.findCommitByLabel = findCommitByLabel;
exports.findCommitByHash = findCommitByHash;
exports.createCommit = createCommit;
exports.getSchema = getSchema;
exports.getBranchInfo = getBranchInfo;
exports.createBranch = createBranch;

function _is_internal_database_name(name) {
  let ret = false;
  if (name == 'branchdb' || name == 'postgres') {
    ret = true;
  }
  return ret;
}
function _is_reserved_database_name(name) {
  let ret = false;
  if (_is_internal_database_name(name) || name == 'public') {
    ret = true;
  }
  return ret;
}
function _is_valid_db_name(name) {
  let ret = true;
  if (_is_reserved_database_name(name)) {
    ret = false;
  } else if (!DB_NAME_REGEX.test(name)) {
    ret = false;
  }
  return ret;
}

function createDatabase(opts,done) {
  const { name, user_name } = opts;
  if (!_is_valid_db_name(name)) {
    done(pg_errors.INVALID_DB_NAME_ERROR)
  } else {
    const sql =
`BEGIN;
CREATE SCHEMA $1i;
SET search_path = $1l;
CREATE TABLE _commit
  (
    commit_id BIGINT PRIMARY KEY NOT NULL,
    commit_hash CHAR(64) NOT NULL,
    user_name VARCHAR(256) NOT NULL,
    commit_log TEXT NOT NULL
  );
CREATE INDEX commit_commit_hash_index ON _commit(commit_hash);
CREATE TABLE _branch
  (
    branch_number SERIAL PRIMARY KEY NOT NULL,
    parent_commit_id BIGINT NOT NULL
  );
CREATE TABLE _label
  (
    label VARCHAR(256) PRIMARY KEY NOT NULL,
    is_tag BOOL DEFAULT FALSE NOT NULL,
    commit_id BIGINT NOT NULL
  );
CREATE TABLE _schema
  (
    commit_id BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    schema_name VARCHAR(256) NOT NULL
  );
CREATE TABLE _table
  (
    commit_id BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL
  );
CREATE TABLE _column
  (
    commit_id BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    column_name VARCHAR(256) NOT NULL,
    column_uuid UUID NOT NULL,
    pg_type INT NOT NULL,
    has_default BOOL DEFAULT FALSE NOT NULL,
    default_value TEXT NULL DEFAULT NULL
  );
INSERT INTO _label VALUES ('master',FALSE,0);
INSERT INTO _commit VALUES (0,'0000000000000000000000000000000000000000000000000000000000000000',$2l,'');
COMMIT;
`
    parent_db.queryPreparse(sql,[name,user_name],(err,res) => {
      if (err && err.code == '42P06') {
        done(pg_errors.DUP_DB_NAME_ERROR);
      } else if (err) {
        console.error("createDatabase: Query failed:",err);
        done(pg_errors.internal(err));
      } else {
        done();
      }
    });
  }
}

function dropDatabase(opts,done) {
  const { name } = opts;
  if (_is_reserved_database_name(name)) {
    done(pg_errors.INVALID_DB_NAME_ERROR)
  } else {
    const sql = `DROP SCHEMA $1i CASCADE;`;
    parent_db.queryPreparse(sql,[name],(err,res) => {
      if (err && err.code == '3F000') {
        done(pg_errors.DB_DOES_NOT_EXIST_ERROR);
      } else if (err) {
        console.error("dropDatabase: Query failed:",err);
        done(pg_errors.internal(err));
      } else {
        done();
      }
    });
  }
}

function findDatabase(name,done) {
  if (!_is_valid_db_name(name)) {
    setImmediate(() => done('invalid_name'));
  } else {
    const sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1";
    parent_db.query(sql,[name],(err,res) => {
      let database = false;
      if (err) {
        console.error("data_store.findDatabase: sql err:",err);
      } else if (res.rows.length == 0) {
        err = 'db_not_found';
      } else {
        database = new Database({ name });
      }
      done(err,database);
    });
  }
}

function findCommit(opts,done) {
  const { database, commit_id } = opts;

  const sql = "SELECT * FROM $1i._commit WHERE commit_id = $2l";
  parent_db.queryPreparse(sql,[database,commit_id],(err,res) => {
    let commit;
    if (err) {
      console.error("data_store.findCommit: sql err:",err);
    } else if (res.rows.length == 0) {
      err = 'commit_not_found';
    } else {
      const row0 = res.rows[0];
      commit = new Commit(row0);
    }
    done(err,commit);
  });
}

function findCommitByLabel(opts,done) {
  const { database, label } = opts;

  const match_label = label.toLowerCase();

  const sql =
`
SELECT c.*,l.is_tag
FROM $1i._label l
JOIN $1i._commit c ON
 ( l.is_tag = TRUE
   AND c.commit_id = l.commit_id
 )
 OR
 (
   l.is_tag = FALSE
   AND c.commit_id >= l.commit_id
   AND c.commit_id < (l.commit_id::bit(64) | B'0000000000000000000000000000000011111111111111111111111111111111')::bigint
 )
WHERE
 l.label LIKE $2l
ORDER BY c.commit_id DESC
LIMIT 1;
;
`
  parent_db.queryPreparse(sql,[database,match_label],(err,res) => {
    let commit;
    let is_tag;
    if (err) {
      console.error("data_store.findCommit: sql err:",err);
    } else if (res.rows.length == 0) {
      err = 'label_not_found';
    } else {
      const row0 = res.rows[0];
      is_tag = row0.is_tag;
      commit = new Commit(row0);
    }
    done(err,commit,is_tag);
  });

}

function findCommitByHash(opts,done) {
  const { database, hash } = opts;
  const like_hash = hash.toLowerCase() + "%";
  const sql = "SELECT * FROM $1i._commit WHERE commit_hash LIKE $2l";
  parent_db.queryPreparse(sql,[database,like_hash],(err,res) => {
    let commit;
    if (err) {
      console.error("data_store.findCommitByHash: sql err:",err);
    } else if (res.rows.length == 0) {
      err = 'hash_not_found';
    } else {
      const row0 = res.rows[0];
      commit = new Commit(row0);
    }
    done(err,commit);
  });
}

function createCommit(opts,done) {
  const {
    base_commit,
    connection,
    database,
    operation_list,
    branch_mode,
  } = opts;

  const db_name = database.getName();
  const user_name = connection.getUserName();

  const log_list = [];
  operation_list.forEach(op => {
    const { log, args } = op;
    log_list.push(parent_db.preparse(log,args));
  });
  const commit_log = log_list.join(";");
  const commit_hash = base_commit.nextHash(commit_log);

  let client = null;
  let next_commit = null;
  let next_commit_id = null;
  async.series([
  (done) => {
    const sql = parent_db.preparse("BEGIN;SET LOCAL search_path = $1i;",[db_name]);
    parent_db.queryWithClient(sql,(err,res,c) => {
      if (err) {
        console.error("Transaction.commit: begin err:",err);
      }
      client = c;
      done(err);
    });
  },
  (done) => {
    const opts = {
      client,
      commit: base_commit,
      branch_mode,
      commit_hash,
    };
    database.reserveCommit(opts,(err,next) => {
      next_commit = next;
      done(err);
    });
  },
  (done) => {
    next_commit_id = next_commit.toIntString();

    const value = {
      commit_id: next_commit_id,
      commit_hash,
      user_name,
      commit_log,
    };
    const sql = parent_db.preparse("INSERT INTO _commit $1v",[value]);
    client.query(sql,(err,res) => {
      if (err) {
        console.error("createCommit: create commit err:",err);
      }
      done(err);
    });
  },
  (done) => {
    const sql_list = [];
    operation_list.forEach(op => {
      const { sql, args } = _map_db_op(op);
      args.push(next_commit_id);
      sql_list.push(parent_db.preparse(sql,args));
    });

    const sql = sql_list.join(";");
    client.query(sql,(err) => {
      if (err) {
        console.error("createCommit: execute sql:",sql,",err:",err);
      }
      done(err);
    });
  },
  (done) => {
    parent_db.commit(client,(err) => {
      if (err) {
        console.error("createCommit: commit err:",err);
      }
      done(err);
    });
  }],
  (err) => {
    if (err) {
      parent_db.rollback(client);
      if (next_commit) {
        database.cancelCommit(next_commit);
      }
    } else {
      database.finishCommit(next_commit);
    }
    done(err,next_commit);
  });
}

function _map_db_op(op) {
  const { cmd } = op;
  let sql;
  let args;

  switch(cmd) {
    case 'CREATE_SCHEMA':
      sql = "INSERT INTO _schema (commit_id,schema_name) VALUES ($2l,$1l)";
      args = op.args;
      break;
    case 'DROP_SCHEMA':
      sql = "INSERT INTO _schema (commit_id,is_deleted,schema_name) VALUES ($2l,TRUE,$1l)";
      args = op.args;
      break;
    default:
      console.error("data_store: unknown operation:",op);
      break;
  }

  return {
    sql,
    args,
  };
}

function getSchema(opts,done) {
  const { database, commit, branch_path } = opts;

  const commit_predicate = _get_commit_predicate(commit,branch_path);

  const sql =
`
BEGIN READ ONLY;
SET search_path = $1l;
SELECT *
  FROM _schema
  WHERE ${commit_predicate}
  ORDER BY commit_id ASC;
SELECT *
  FROM _table
  WHERE ${commit_predicate}
  ORDER BY commit_id ASC;
ROLLBACK;
`
  parent_db.queryPreparse(sql,[database],(err,res) => {
    let schema = false;
    if (err) {
      console.error("data_store.getSchema: sql err:",err);
    } else if (res.length < 4) {
      err = 'invalid_parent_result';
    } else {
      const schema_rows = res[2].rows;
      const table_rows = res[3].rows;

      const schema_map = {};
      const table_map = {};

      schema_rows.forEach((r) => {
        const { schema_name, is_deleted, commit_id } = r;
        if (is_deleted) {
          delete schema_map[schema_name];
        } else {
          schema_map[schema_name] = {
            schema_name,
            commit_id,
          };
        }
      });
      table_rows.forEach((r) => {
        const { schema_name, table_name, is_deleted, commit_id } = r;
        const schema_table = schema_name + "." + table_name;
        if (is_deleted) {
          delete table_map[schema_table];
        } else {
          const schema = schema_map[schema_name];
          if (schema && schema.commit_id <= commit_id) {
            table_map[schema_table] = {
              schema_table,
              schema_name,
              table_name,
              commit_id,
            };
          }
        }
      });

      schema = {
        schema_map,
        table_map,
      };
    }
    done(err,schema);
  });
}

function getBranchInfo(opts,done) {
  const { database, commit } = opts;
  const commit_id = commit.toIntString();
  const branch_number = commit.getBranchNumber();
  const branch_min = commit.getBranchMin();
  const branch_max = commit.getBranchMax();

  const sql =
`
BEGIN READ ONLY;
SET search_path = $1l;
SELECT
  *
  FROM _commit
  WHERE commit_id BETWEEN $2l AND $3l
  ORDER BY commit_id DESC
  LIMIT 1;
WITH RECURSIVE recursive_branches(branch_number,parent_commit_id,parent_branch_number) AS (
  SELECT
    branch_number,
    parent_commit_id,
    parent_commit_id >> 32 AS parent_branch_number
  FROM _branch
  WHERE branch_number = $4l
  UNION ALL
  SELECT
    b1.branch_number,
    b1.parent_commit_id,
    b1.parent_commit_id >> 32 AS parent_branch_number
  FROM _branch b1
  JOIN recursive_branches b2 ON b1.branch_number = b2.parent_branch_number
  )
  SELECT *
  FROM recursive_branches
  ORDER BY branch_number DESC;
ROLLBACK;
`
  const args = [database,branch_min,branch_max,branch_number];
  parent_db.queryPreparse(sql,args,(err,res) => {
    let branch_info;
    if (err) {
      console.error("data_store.getBranchInfo: sql err:",err);
    } else if (res.length < 4) {
      err = 'invalid_parent_result';
    } else {
      try {
        const head_row0 = res[2].rows[0];
        const head_commit = new Commit(head_row0);
        const branch_rows = res[3].rows;

        const branch_path = [];
        branch_rows.forEach(r => {
          const { branch_number, parent_commit_id, parent_branch_number } = r;
          const parent_commit = new Commit(parent_commit_id);

          branch_path.push({
            parent_commit,
            branch_min: parent_commit.getBranchMin(),
            branch_max: parent_commit_id,
          });
        });

        branch_info = {
          head_commit,
          branch_path,
        };
      } catch(e) {
        console.error("data_store.getBranchInfo: exception:",e.stack);
        err = 'invalid_branch';
      }
    }
    done(err,branch_info);
  });
}

function createBranch(opts,done) {
  const { client, database, parent_commit } = opts;

  const parent_commit_id = parent_commit.toIntString();

  const value = {
    parent_commit_id,
  };
  const sql = "INSERT INTO $1i._branch $2v RETURNING branch_number";
  const new_sql = parent_db.preparse(sql,[database,value]);
  client.query(new_sql,(err,res) => {
    let branch_number;
    if (err) {
      console.error("data_store.createBranch: sql err:",err);
    } else if (res.rows.length == 0) {
      err = 'bad_branch_create';
    } else {
      branch_number = res.rows[0].branch_number;
    }
    done(err,branch_number);
  });
}

function _get_commit_predicate(commit,branch_path) {
  const commit_id = commit.toIntString();
  const branch_min = commit.getBranchMin();

  let branch_sql = "";
  if (branch_path.length > 0) {
    branch_sql = " OR " + branch_path.map(b => {
      const { branch_min, branch_max } = b;
      const sql = "(commit_id >= $1l AND commit_id <= $2l)";
      return parent_db.preparse(sql,[branch_min,branch_max]);
    }).join(" OR ");
  }

  const sql =
`
commit_id <= $1l
AND (
 commit_id >= $2l
 ${branch_sql}
 )
`
  const ret = parent_db.preparse(sql,[commit_id,branch_min]);
  return ret;
}
