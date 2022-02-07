-- Please note: tests are designed to be run in parallel so create tables per test to avoid collisions
CREATE TABLE IF NOT EXISTS ziocassandrasessionspec_executeAction (
    id INT PRIMARY KEY,
    info TEXT
);

CREATE TABLE IF NOT EXISTS ziocassandrasessionspec_executeBatchAction (
    id INT PRIMARY KEY,
    info TEXT
);
