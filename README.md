[![Build Status](https://circleci.com/gh/letsencrypt/mariadb-sequential-partition-manager-py.svg?style=shield)](https://circleci.com/gh/letsencrypt/mariadb-sequential-partition-manager-py)
![Maturity Level: Beta](https://img.shields.io/badge/maturity-beta-blue.svg)

This tool partitions and manages MariaDB tables by sequential IDs.

This is primarily a mechanism for dropping large numbers of rows of data without using `DELETE` statements.

Adding partitions in the first place with InnoDB requires a full table copy. Otherwise, the `REORGANIZE PARTITION` command is fast only if operating on a partition that is empty, e.g., has no rows.

Similar tools:
* https://github.com/davidburger/gomypartition, intended for tables with date-based partitions
* https://github.com/yahoo/mysql_partition_manager, which is archived and in pure SQL

# Usage

```sh
 → pip install --editable .
 → partition-manager --log-level=debug  \
    --mariadb test_tools/fake_mariadb.sh \
    add --noop --table tablename
DEBUG:root:Auto_Increment column identified as id
DEBUG:root:Partition range column identified as id
DEBUG:root:Found partition before = (100)
DEBUG:root:Found tail partition named p_20201204
INFO:root:No-op mode

ALTER TABLE `dbname`.`tablename` REORGANIZE PARTITION `p_20201204` INTO (PARTITION `p_20201204` VALUES LESS THAN (3101009), PARTITION `p_20210122` VALUES LESS THAN MAXVALUE);

```

You can also use a yaml configuration file with the `--config` parameter of the form:
```yaml
partitionmanager:
  dburl: sql://user:password@localhost/db-name
  # or
  # mariadb: /usr/local/bin/mariadb
  partition_period:
    days: 7
  num_empty: 2

  tables:
    table1:
      retention:
        days: 60
    table2:
      partition_period:
        days: 30
    table3:
      retention:
        days: 14
```

For tables which are either partitioned but not yet using this tool's schema, or which have no empty partitions, the `bootstrap` command can be useful for proposing alterations to run manually. Note that `bootstrap` proposes commands that are likely to require partial copies of each table, so likely they will require a maintenance period.

```sh
partition-manager --mariadb ~/bin/rootsql-dev-primary bootstrap --out /tmp/bootstrap.yml --table orders
INFO:write_state_info:Writing current state information
INFO:write_state_info:(Table("orders"): {'id': 9236}),

# wait some time
partition-manager --mariadb ~/bin/rootsql-dev-primary bootstrap --in /tmp/bootstrap.yml --table orders
INFO:calculate_sql_alters:Reading prior state information
INFO:calculate_sql_alters:Table orders, 24.0 hours, [9236] - [29236], [20000] pos_change, [832.706363653845]/hour
orders:
 - ALTER TABLE `orders` REORGANIZE PARTITION `p_20210405` INTO (PARTITION `p_20210416` VALUES LESS THAN (30901), PARTITION `p_20210516` VALUES LESS THAN (630449), PARTITION `p_20210615` VALUES LESS THAN MAXVALUE);

```

# Algorithm

The core algorithm is implemented in a method `plan_partition_changes` in `table_append_partition.py`. That algorithm is:

For a given table and that table's intended partition period, desired end-state is to have:
- All the existing partitions containing data,
- A configurable number of trailing partitions which contain no data, and
- An "active" partition currently being filled with data

To make it easier to manage, we give all the filled partitions a name to indicate the approximate date that partition began being filled with data. This date is approximate because once a partition contains data, it is no longer an instant `ALTER` operation to rename the partition, rather every contained row gets copied, so this tool predicts the date at which the new partition will become the "active" one.

Inputs:
- The table name
- The intended partition period
- The number of trailing partitions to keep
- The table's current partition list
- The table's partition id's current value(s)

Outputs:
- An intended partition list, changing only the empty partitions, or
- If no partitions can be reorganized, an error.

Procedure:
- Using the current values, split the partition list into two sub-lists: empty partitions, and non-empty partitions.
- If there are no empty partitions:
  - Raise an error and halt the algorithm.

- Perform a statistical regression using each non-empty partition to determine each partition's fill rate.
- Using each partition's fill rate and their age, predict the future partition fill rate.
- Create a new list of intended empty partitions.
- For each empty partition:
  - Predict the start-of-fill date using the partition's position relative to the current active partition, the current active partition's date, the partition period, and the future partition fill rate.
  - If the start-of-fill date is different than the partition's name, rename the partition.
  - Append the changed partition to the intended empty partition list.
- While the number of empty partitions is less than the intended number of trailing partitions to keep:
  - Predict the start-of-fill date for a new partition using the previous partition's date and the partition period.
  - Append the new partition to the intended empty partition list.
- Return the lists of non-empty partitions, the current empty partitions, and the post-algorithm intended empty partitions.

# TODOs

Lots:
[X] Support for tables with partitions across multiple columns.
[ ] A drop mechanism, for one. Initially it should take a retention period and log proposed `DROP` statements, not perform them.
[ ] Yet more tests, particularly live integration tests with a test DB.
