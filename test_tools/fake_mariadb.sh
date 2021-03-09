#!/bin/bash
stdin=$(cat)

if echo "$*" | grep "v" >/dev/null; then
  echo "mariadb command was: $@" >&2
  echo "stdin was: $stdin" >&2
fi

if echo $stdin | grep "INFORMATION_SCHEMA" >/dev/null; then
  if echo $stdin | grep "unpartitioned" >/dev/null; then
    cat <<EOF
<?xml version="1.0"?>

<resultset statement="SELECT AUTO_INCREMENT, CREATE_OPTIONS FROM
                  INFORMATION_SCHEMA.TABLES" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
    <field name="AUTO_INCREMENT">3101009</field>
    <field name="CREATE_OPTIONS">max_rows=10380835156842741 transactional=0</field>
  </row>
</resultset>
EOF
    exit
  else
	 cat <<EOF
<?xml version="1.0"?>

<resultset statement="SELECT AUTO_INCREMENT, CREATE_OPTIONS FROM
                  INFORMATION_SCHEMA.TABLES" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
    <field name="AUTO_INCREMENT">3101009</field>
    <field name="CREATE_OPTIONS">max_rows=10380835156842741 transactional=0 partitioned</field>
  </row>
</resultset>
EOF
	exit
  fi
fi

if echo $stdin | grep "ORDER BY" >/dev/null; then
  cat <<EOF
<?xml version="1.0"?>

<resultset statement="SELECT id FROM burgers ORDER BY id DESC LIMIT 1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
  <field name="id">3101009</field>
  </row>
</resultset>
EOF
  exit
fi

if echo $stdin | grep "SHOW CREATE" >/dev/null; then
  if echo $stdin | grep "partitioned_last_week" >/dev/null; then
    midPartName=$(date --utc --date='37 days ago' +p_%Y%m%d)
    tailPartName=$(date --utc --date='7 days ago' +p_%Y%m%d)
  elif echo $stdin | grep "partitioned_yesterday" >/dev/null; then
    midPartName=$(date --utc --date='31 days ago' +p_%Y%m%d)
    tailPartName=$(date --utc --date='yesterday' +p_%Y%m%d)
  else
    midPartName="p_20201105"
    tailPartName="p_20201204"
  fi

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
 PARTITION \`${midPartName}\` VALUES LESS THAN (1000) ENGINE = InnoDB,
 PARTITION \`${tailPartName}\` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)</field>
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
    <field name="DATABASE()">tasty-treats</field>
  </row>
</resultset>
EOF
    exit
fi

exit 1
