'use strict';

const crypto = require('crypto');
const bigint = require('big-integer');

module.exports = Commit;

function Commit(opts) {
  if (this instanceof Commit) {
    const { commit_id, commit_hash, branch_number, commit_number } = opts;

    let s;
    if (commit_id) {
      const info = stringToCommitInfo(commit_id);
      this._commit_id = info.commit_id;
      this._branch_number = info.branch_number;
      this._commit_number = info.commit_number;
    } else if (typeof branch_number == 'number' && typeof commit_number == 'number') {
      this._branch_number = branch_number;
      this._commit_number = commit_number;
      this._commit_id = bigint(branch_number).shiftLeft(32).add(commit_number).toString();
    } else {
      s = opts.toString();
      const info = stringToCommitInfo(s);
      if (!info) {
        throw new Error("Commit.Commit: bad new commit:" + opts);
      }
      this._commit_id = info.commit_id;
      this._branch_number = info.branch_number;
      this._commit_number = info.commit_number;
    }
    if (commit_hash) {
      this._commit_hash = commit_hash;
    } else if (this._commit_id === "0") {
      this._commit_hash = Array(64+1).join("0")
    }
  } else {
    return new Commit(opts);
  }
}

Commit.prototype.toString = function() {
  return this._commit_id;
};

Commit.prototype.toIntString = function() {
  return this._commit_id;
};
Commit.prototype.toHexString = function() {
  const ret =
    this._branch_number.toString(16)
    + "::"
    + this._commit_number.toString(16);
  return ret;
};
Commit.prototype.getHashString = function() {
  return this._commit_hash.toString('hex');
};

Commit.prototype.nextHash = function(next_log) {
  const hash = crypto.createHash('sha512');
  hash.update(this._commit_hash);
  hash.update(next_log);
  return hash.digest('hex').slice(64);
};
Commit.prototype.getNextCommit = function(commit_hash) {
  const branch_number = this._branch_number;
  const commit_number = this._commit_number + 1;
  return new Commit({ branch_number, commit_number, commit_hash });
};

Commit.stringToCommitInfo = stringToCommitInfo;

function stringToCommitInfo(s) {
  let commit_id;
  let commit_number;
  let branch_number;
  try {
    if (typeof s == 'string' && s.indexOf("::") != -1) {
      const match = s.match(/^([0-9a-fA-F]{1,8})::([0-9a-fA-F]{1,8})$/);
      if (match) {
        branch_number = parseInt(match[1],16);
        commit_number = parseInt(match[2],16);
        commit_id = bigint(branch_number).shiftLeft(32).add(commit_number).toString();
      }
    } else if (typeof s == 'number' || (typeof s == 'string' && /^\d*$/.test(s)) ) {
        commit_number = bigint(s).and(0xffffffff).toJSNumber();
        branch_number = bigint(s).shiftRight(32).and(0xffffffff).toJSNumber();
        commit_id = s.toString();
    }
  } catch(e) {
    console.error("Commit.stringToCommitInfo: throw:",e.stack);
  }
  let ret;
  if (commit_id) {
    ret = { commit_id, branch_number, commit_number };
  }
  return ret;
}

function _pad_zero(s) {
  return ("00000000" + s).slice(-8);
}

if (require.main === module) {
  [
  0x100000010,
  0x000000010,
  0x4200000042,
  "5::20",
  "foo",
  "foo::bar",
  0,
  "0",
  ].forEach(s => {
    console.log(s + " -> " + JSON.stringify(stringToCommitInfo(s),null,""));
  });
}
