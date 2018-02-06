'use strict';

const PgServer = require('node-postgres-server');
const Connection = require('./src/connection.js');

const server = new PgServer();

server.on('listening',() => {
  console.log("Server listening")
});
server.on('new_client',pg_client => new Connection(pg_client));
server.on('close',() => {
  console.log("Server close");
});
server.on('error',err => {
  console.error("Server error:",err);
});

server.listen(5432,(err) => {
  console.log("BranchDB Server listen started on :5432");
});
