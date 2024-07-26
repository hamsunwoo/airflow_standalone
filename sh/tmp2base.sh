#!/bin/bash

DT=$1

user="root"
database="history_db"

MYSQL_PWD="qwer123" mysql --local-infile=1 -u"$user" "$database"<<EOF
DELETE FROM history_db.cmd_usage WHERE dt='${DT}';

INSERT INTO cmd_usage
SELECT
	CASE WHEN dt LIKE '%-%-%'
			THEN str_to_date(dt, '%Y-%m-%d')
			ELSE str_to_date('1970-01-01', '%Y-%m-%d')
	END as dt,
	command,
	CASE WHEN cnt REGEXP '[0-9]+$'
			THEN cast(cnt as unsigned)
			ELSE -1
	END as cnt,
	'${DT}' as tmp_dt
FROM tmp_cmd_usage
WHERE dt = '${DT}';
EOF


