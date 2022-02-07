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
)
