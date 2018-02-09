'use strict';

const grammar = require('../lib/grammar.js');
const tracer = require('../lib/tracer.js');

exports.parse = parse;
exports.SyntaxError = grammar.SyntaxError;

function parse(query) {
  const t = new tracer.Tracer();

  const opts = {
    tracer: t,
  };

  let ret;
  try {
    ret = grammar.parse(query,opts);
  } catch (e) {
    throw e instanceof grammar.SyntaxError ? t.smartError(e) : e;
  }
  return ret;
}
