-- Please note: tests are designed to be run in parallel so create tables per test to avoid collisions
CREATE TABLE IF NOT EXISTS ziocassandrasessionspec_executeAction
(
    id   INT PRIMARY KEY,
    info TEXT
);

CREATE TABLE IF NOT EXISTS ziocassandrasessionspec_executeBatchAction
(
    id   INT PRIMARY KEY,
    info TEXT
);

CREATE TABLE IF NOT EXISTS ziocassandrasessionspec_selectPage
(
    id     INT,
    bucket INT,
    info   TEXT,
    PRIMARY KEY ((id), bucket)
);

CREATE TABLE IF NOT EXISTS ziocassandrasessionspec_timeoutcheck
(
    id           INT PRIMARY KEY,
    info         TEXT,
    another_info TEXT
);

CREATE TYPE udt_address (
    number INT,
    street TEXT,
    city TEXT,
    );

CREATE TYPE udt_email (
    username TEXT,
    domain TEXT,
    domain_name TEXT
    );

CREATE TYPE udt_data (
    addresses frozen<list<udt_address>>,
    email frozen<udt_email>
    );

CREATE TABLE IF NOT EXISTS userdefinedtypesspec_person
(
    id   INT PRIMARY KEY,
    name TEXT,
    age  INT,
    data frozen<udt_data>
);

CREATE TYPE example_type (
    x bigint,
    y bigint,
    date date,
    time time
    );

CREATE TYPE example_nested_type(
    a int,
    b text,
    c frozen<example_type>
    );

CREATE TYPE example_collection_nested_udt_type(
    a int,
    b frozen<map<int,frozen<set<frozen<set<frozen<set<frozen<set<example_nested_type>>>>>>>>>>,
    c frozen<example_nested_type>
    );

CREATE TABLE userdefinedtypesspec_heavilynestedudttable
(
    id   int,
    data example_collection_nested_udt_type,
    PRIMARY KEY (id)
);

CREATE TABLE collectionspec_simplecollectiontable
(
    id        INT PRIMARY KEY,
    map_test  FROZEN<MAP<INT,TEXT>>,
    set_test  FROZEN<SET<BIGINT>>,
    list_test FROZEN<LIST<TEXT>>
);

CREATE TABLE collectionspec_optioncollectiontable
(
    id            INT PRIMARY KEY,
    opt_map_test  FROZEN<MAP<INT,TEXT>>,
    opt_set_test  FROZEN<SET<BIGINT>>,
    opt_list_test FROZEN<LIST<TEXT>>
);

CREATE TABLE collectionspec_nestedcollectiontable
(
    a int PRIMARY KEY,
    b frozen<map<int, frozen<set<frozen<set<frozen<set<frozen<set<int>>>>>>>>>>
);

CREATE TYPE cursorspec_note(
    data TEXT,
    ip INET
    );

CREATE TYPE cursorspec_address(
    street TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    note frozen<cursorspec_note>
    );

CREATE TABLE cursorspec_cursorexampletable
(
    id           BIGINT PRIMARY KEY,
    name         TEXT,
    may_be_empty TEXT,
    age          SMALLINT,
    addresses    frozen<list<cursorspec_address>>,
);

CREATE TABLE updatebuilderspec_person
(
    id   INT PRIMARY KEY,
    name TEXT,
    age  INT
);

CREATE TABLE relationspec_person(
    id   INT PRIMARY KEY,
    name TEXT,
    age  INT
);
