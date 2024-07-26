CREATE DATABASE IF NOT EXISTS history_db;

USE history_db;

create table if not exists cmd_usage(
	dt DATE,
	command VARCHAR(500),
	cnt INT
);

