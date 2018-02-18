'use strict';

exports.BRANCH_COMMIT_REGEX = /^([0-9a-fA-F]{1,8})::([0-9a-fA-F]{1,8})$/;
exports.LABEL_NAME_REGEX = /^[\w_-]*$/;
exports.COMMIT_HASH_REGEX = /^[\dA-Fa-f]*$/;
exports.SCHEMA_NAME_REGEX = /^[\w-]+$/;
exports.DB_NAME_REGEX = /^[\w-]+$/;
exports.DB_CONNECT_SPEC_REGEX = /^([^:]*):?(.*)?$/;

const BUILT_IN_SCHEMA = {
  schema_map: {},
  table_map: {},
};
exports.BUILT_IN_SCHEMA = BUILT_IN_SCHEMA;

const IS_SCHEMATA_COLUMNS = [
  { column_name: 'catalog_name', type: 'text', },
  { column_name: 'schema_name', type: 'text', },
  { column_name: 'schema_owner', type: 'text', },
  { column_name: 'default_character_set_catalog', type: 'text', },
  { column_name: 'default_character_set_schema', type: 'text', },
  { column_name: 'default_character_set_name', type: 'text', },
  { column_name: 'sql_path', type: 'text', },
];

_make_built_in_schema('information_schema');
_make_built_in_table('information_schema','schemata',IS_SCHEMATA_COLUMNS);

function _make_built_in_schema(schema_name) {
  BUILT_IN_SCHEMA.schema_map[schema_name] = {
    schema_name,
    built_in: true,
    commit_id: "0",
    table_map: {},
  };
}

function _make_built_in_table(schema_name,table_name,column_list) {
  BUILT_IN_SCHEMA.schema_map[schema_name].table_map[table_name] = true;
  const schema_table = schema_name + "." + table_name;

  const column_map = {};
  column_list.forEach(c => {
    column_map[c.column_name] = c;
  });

  BUILT_IN_SCHEMA.table_map[schema_table] = {
    schema_table,
    column_map,
    column_list
  };
}
