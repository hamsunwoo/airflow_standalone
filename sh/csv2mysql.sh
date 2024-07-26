#!/bin/bash

CSV_FILE=$1
DEL_DT=$2

user="root"
database="history_db"

MYSQL_PWD="qwer123" mysql --local-infile=1 -u"$user" "$database"<<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

LOAD DATA LOCAL INFILE '$CSV_FILE'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1
FIELDS TERMINATED BY ',' ESCAPED BY '' ENCLOSED BY '^'
LINES TERMINATED BY '\n';
EOF
