'use strict';

const _ = require('lodash');
const async = require('async');
const pg_errors = require('./pg_errors.js');
const Commit = require('./commit.js');
const data_store = require('./data_store.js');
const {
  BRANCH_COMMIT_REGEX,
  LABEL_NAME_REGEX,
  COMMIT_HASH_REGEX,
  SCHEMA_NAME_REGEX,
} = require('./constants.js');

module.exports = Database;

const g_database_map = {};

new Database({ name: 'branchdb', is_internal: true });
new Database({ name: 'postgres', is_internal: true });

function findDatabase(name,done) {
  let ret = false;
  if (name in g_database_map) {
    const db = g_database_map[name];
    done(null,db);
  } else {
    data_store.findDatabase(name,done);
  }
  return ret;
}

function Database(opts) {
  if (this instanceof Database) {
    this._name = opts.name;
    this._is_internal = !!opts.is_internal;
    this._schema_cache = {};
    this._branch_map = {};
    g_database_map[opts.name] = this;
  } else {
    return new Database(opts);
  }
}
Database.findDatabase = findDatabase;

Database.prototype.getName = function() {
  return this._name;
};
Database.prototype.isInternal = function() {
  return this._is_internal;
};

Database.prototype.createSchema = function(opts,done) {
  const { name, transaction, } = opts;
  if (!SCHEMA_NAME_REGEX.test(name)) {
    done(pg_errors.SYNTAX_ERROR);
  } else {
    this.getSchema(transaction,(err,schema) => {
      if (!err) {
        if (schema.schema_map[name]) {
          err = pg_errors.DUP_SCHEMA_NAME_ERROR;
        } else {
          const cmd = 'CREATE_SCHEMA';
          const log = "CREATE SCHEMA $1i";
          const args = [name];
          const schema_update = { cmd, name };
          const op = { cmd, log, args, schema_update };
          transaction.addOperation(op);
        }
      }
      done(err);
    });
  }
};

Database.prototype.dropSchema = function(opts,done) {
  const { name, transaction, } = opts;
  this.getSchema(transaction,(err,schema) => {
    if (!err) {
      if (!schema.schema_map[name]) {
        err = pg_errors.SCHEMA_DOES_NOT_EXIST_ERROR;
      } else {
        const cmd = 'DROP_SCHEMA';
        const log = "DROP SCHEMA $1i";
        const args = [name];
        const schema_update = { cmd, name };
        const op = { cmd, log, args, schema_update };
        transaction.addOperation(op);
      }
    }
    done(err);
  });
};

Database.prototype.createTable = function(opts,done) {
  done(pg_errors.NOT_IMPLEMENTED_ERROR);
};

Database.prototype.getCommitByString = function(s,done) {
  if (this._is_internal) {
    done('commit_not_found');
  } else {
    const is_branch_commit = BRANCH_COMMIT_REGEX.test(s);
    const maybe_label = LABEL_NAME_REGEX.test(s);
    const maybe_hash = COMMIT_HASH_REGEX.test(s);

    let commit;
    let rev_type;
    async.series([
    (done) => {
      if (is_branch_commit) {
        this._getCommitByBranchCommit(s,(err,c) => {
          commit = c;
          rev_type = 'commit_id';
          // this excludes the next 2 cases because they are definitionally false
          done(err);
        });
      } else {
        done();
      }
    },
    (done) => {
      if (!commit && maybe_label) {
        this._getCommitByLabel(s,(err,c,is_tag) => {
          if (err == 'label_not_found') {
            err = null;
          } else  {
            commit = c;
            rev_type = is_tag ? 'tag' : 'branch';
          }
          done(err);
        });
      } else {
        done();
      }
    },
    (done) => {
      if (!commit && maybe_hash) {
        this._getCommitByHash(s,(err,c) => {
          commit = c;
          rev_type = 'hash';
          done(err,commit,'hash');
        });
      } else {
        done();
      }
    }],
    (err) => {
      if (!err && !commit) {
        err = 'commit_not_found';
      }
      if (err) {
        done(err);
      } else {
        this._loadBranchInfo(commit,(err) => {
          done(err,commit,rev_type);
        });
      }
    });
  }
};

Database.prototype._getCommitByLabel = function(label,done) {
  const opts = { database: this._name, label };
  data_store.findCommitByLabel(opts,done);
};
Database.prototype._getCommitByHash = function(hash,done) {
  const opts = { database: this._name, hash };
  data_store.findCommitByHash(opts,done);
};
Database.prototype._getCommitByBranchCommit = function(s,done) {
  const info = Commit.stringToCommitInfo(s)
  if (!info) {
    done('invalid_id');
  } else if (info.commit_id == 0) {
    done(null,new Commit(0));
  } else {
    const { commit_id } = info;

    const opts = {
      database: this._name,
      commit_id,
    };
    data_store.findCommit(opts,done);
  }
};

Database.prototype.getSchema = function(transaction,done) {
  const commit = transaction.getCommit();
  const commit_id = commit.toString();

  let base_schema = false;
  let schema = false;
  async.series([
  (done) => {
    if (commit_id in this._schema_cache) {
      base_schema = this._schema_cache[commit_id];
      done(null);
    } else {
      const { branch_path } = this._getBranch(commit);
      const opts = {
        database: this._name,
        commit,
        branch_path,
      };
      data_store.getSchema(opts,(err,schema) => {
        if (!err) {
          this._schema_cache[commit_id] = schema;
          base_schema = schema;
        }
        done(err);
      });
    }
  }],(err) => {
    if (!err) {
      schema = this._extendSchema(base_schema,transaction);
    }
    done(err,schema);
  });
};
Database.prototype._extendSchema = function(base_schema,transaction) {
  const ret = _.merge({},base_schema);

  const op_list = transaction.getOperationList();

  op_list.forEach(op => {
    if (op.schema_update) {
      const { cmd, name } = op.schema_update;
      switch(cmd) {
        case 'CREATE_SCHEMA':
          ret.schema_map[name] = {
            schema_name: name,
          };
          break;
        case 'DROP_SCHEMA':
          delete ret.schema_map[name];
          _.each(ret.table_map,(t,schema_table) => {
            if (t.schema_name == name) {
              delete ret.table_map[schema_table];
            }
          });
          break;
        default:
          console.error("Database._extendSchema: unhandled schema_update:",op);
          break;
      }
    }
  });

  return ret;
};

Database.prototype.reserveCommit = function(opts,done) {
  const { client, commit, commit_hash, branch_mode } = opts;

  const branch = this._getBranch(commit);

  if (!branch.is_head_reservered && commit.isEqual(branch.head_commit)) {
    branch.is_head_reservered = true;
    done(null,commit.getNextCommit(commit_hash));
  } else if (branch_mode == 'conflict') {
    done('conflict');
  } else if (branch_mode == 'branch') {
    this._createBranch(opts,done);
  } else {
    done('bad_branch_mode');
  }
};
Database.prototype.cancelCommit = function(next_commit) {
  if (next_commit.getCommitNumber() == 0) {
    const branch_number = next_commit.getBranchNumber();
    delete this._branch_map[branch_number];
  }
};
Database.prototype.finishCommit = function(next_commit) {
  const branch = this._getBranch(next_commit);
  branch.head_commit = next_commit;
  branch.is_head_reservered = false;
};

Database.prototype.getBranchHead = function(commit) {
   const branch = this._getBranch(commit);
   const ret = branch.head_commit;
   return ret;
};

Database.prototype._getBranch = function(arg) {
  let branch_number;
  if (arg instanceof Commit) {
    branch_number = arg.getBranchNumber();
  } else if (typeof arg == 'number') {
    branch_number = arg;
  } else {
    throw new Error("Bad branch index");
  }
  if (!(branch_number in this._branch_map)) {
    this._branch_map[branch_number] = {
      branch_number,
    };
  }

  return this._branch_map[branch_number];
};
Database.prototype._loadBranchInfo = function(commit,done) {
  const branch = this._getBranch(commit);

  const opts = {
    database: this._name,
    commit,
  };
  data_store.getBranchInfo(opts,(err,branch_info) => {
    if (!err) {
      branch.head_commit = branch_info.head_commit;
      branch.branch_path = branch_info.branch_path;
    }
    done(err);
  });
};
Database.prototype._createBranch = function(params,done) {
  const { client, commit, commit_hash, } = params;

  const opts = {
    client,
    database: this._name,
    parent_commit: commit,
  };
  data_store.createBranch(opts,(err,branch_number) => {
    let next_commit;
    if (!err) {
      next_commit = commit.getNextBranchCommit(branch_number,commit_hash);
    }
    done(err,next_commit);
  });
};
