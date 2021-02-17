#!/bin/bash
stdin=$(cat)

if echo "$*" | grep "v" >/dev/null; then
  echo "mariadb command was: $@" >&2
  echo "stdin was: $stdin" >&2
fi

if echo $stdin | grep "INFORMATION_SCHEMA" >/dev/null; then
	cat <<EOF
<?xml version="1.0"?>

<resultset statement="SELECT AUTO_INCREMENT, CREATE_OPTIONS FROM
                  INFORMATION_SCHEMA.TABLES" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
    <field name="AUTO_INCREMENT">3101009</field>
    <field name="CREATE_OPTIONS">max_rows=10380835156842741 transactional=0 partitioned</field>
  </row>
EOF
	exit
fi

if echo $stdin | grep "SHOW CREATE" >/dev/null; then
	cat <<EOF
<?xml version="1.0"?>

<resultset statement="show create table burgers" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
    <field name="Table">burgers</field>
    <field name="Create Table">CREATE TABLE \`burgers\` (
  \`id\` bigint(20) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (\`id\`),
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (\`id\`)
(PARTITION \`p_start\` VALUES LESS THAN (10) ENGINE = InnoDB,
 PARTITION \`p_20201204\` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)</field>
  </row>
</resultset>
EOF
	exit
fi

if echo $stdin | grep "REORGANIZE PARTITION" >/dev/null; then
    cat <<EOF
<?xml version="1.0"?>

<resultset statement="REORGANIZE PARTITION yada yada" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
</resultset>
EOF
    exit
fi

if echo $stdin | grep "SELECT DATABASE" >/dev/null; then
    cat <<EOF
<?xml version="1.0"?>

<resultset statement="select database()" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
    <field name="database()">tasty-treats</field>
  </row>
</resultset>

EOF
    exit
fi

exit 1
