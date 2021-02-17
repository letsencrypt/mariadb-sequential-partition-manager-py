#!/bin/bash
stdin=$(cat)

if echo "$*" | grep "v" >/dev/null; then
  echo "mariadb command was: $@" >&2
  echo "stdin was: $stdin" >&2
fi

if echo $stdin | grep "INFORMATION_SCHEMA" >/dev/null; then
	cat <<EOF
*************************** 1. row ***************************
AUTO_INCREMENT: 3101009
CREATE_OPTIONS: partitioned
EOF
	exit
fi

if echo $stdin | grep "SHOW CREATE" >/dev/null; then
	cat <<EOF
*************************** 1. row ***************************
       Table: tablename
Create Table: CREATE TABLE \`tablename\` (
  \`id\` bigint(20) NOT NULL AUTO_INCREMENT,
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (\`id\`)
(PARTITION \`before\` VALUES LESS THAN (100),
PARTITION \`p_20201204\` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)
EOF
	exit
fi

if echo $stdin | grep "REORGANIZE PARTITION" >/dev/null; then
    exit
fi

if echo $stdin | grep "SELECT DATABASE" >/dev/null; then
    cat <<EOF
*************************** 1. row ***************************
DATABASE(): tasty-treats
EOF
    exit
fi

exit 1
