[![Build Status](https://circleci.com/gh/letsencrypt/mariadb-sequential-partition-manager-py.svg?style=shield)](https://circleci.com/gh/letsencrypt/mariadb-sequential-partition-manager-py)
![Maturity Level: Beta](https://img.shields.io/badge/maturity-beta-blue.svg)

This tool partitions and manages MariaDB tables by sequential IDs.

Note that reorganizing partitions is not a fast operation on ext4 filesystems; it is fast on xfs and zfs, but only when the partition being edited contains no rows. Adding partitions in the first place with InnoDB requires a full table copy.

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


# Algorithm

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
  - Predict the end-of-fill value using the start-of-fill date and the future partition fill rate.
  - If the start-of-fill date is different than the partition's name, rename the partition.
  - If the end-of-fill value is different than the partition's current value, change that value.
  - Append the changed partition to the intended empty partition list.
- While the number of empty partitions is less than the intended number of trailing partitions to keep:
  - Predict the start-of-fill date for a new partition using the previous partition's date and the partition period.
  - Predict the end-of-fill value using the start-of-fill date and the future partition fill rate.
  - Append the new partition to the intended empty partition list.
- Return the lists of non-empty partitions, the current empty partitions, and the post-algorithm intended empty partitions.

# TODOs

Lots. A drop mechanism, for one. Yet more tests, particularly live integration tests with a test DB, for another.
