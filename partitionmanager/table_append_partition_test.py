# flake8: noqa: E501

import unittest
import argparse
from partitionmanager.types import (
    DatabaseCommand,
    TableInformationException,
    MismatchedIdException,
    SqlInput,
)
from partitionmanager.table_append_partition import (
    parse_table_information_schema,
    parse_partition_map,
    get_autoincrement,
    get_partition_map,
)


class TestDatabaseCommand(DatabaseCommand):
    def run(self, cmd):
        return ""


class TestTypeEnforcement(unittest.TestCase):
    def test_get_partition_map(self):
        with self.assertRaises(ValueError):
            get_partition_map(TestDatabaseCommand(), "", "")

    def test_get_autoincrementp(self):
        with self.assertRaises(ValueError):
            get_autoincrement(TestDatabaseCommand(), "", "")


class TestParseTableInformationSchema(unittest.TestCase):
    def test_null_auto_increment(self):
        info = """*************************** 1. row ***************************
AUTO_INCREMENT: NULL
CREATE_OPTIONS: partitioned"""
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_not_partitioned(self):
        info = """*************************** 1. row ***************************
AUTO_INCREMENT: 2
CREATE_OPTIONS: exfoliated"""
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_not_partitioned_and_unexpected(self):
        info = """*************************** 1. row ***************************
AUTO_INCREMENT: NULL
CREATE_OPTIONS: exfoliated, disenchanted"""
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_auto_increment_not_int(self):
        info = """*************************** 1. row ***************************
AUTO_INCREMENT: 1.21
CREATE_OPTIONS: jiggawatts, partitioned"""
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_normal(self):
        info = """*************************** 1. row ***************************
AUTO_INCREMENT: 3101009
CREATE_OPTIONS: partitioned"""
        self.assertEqual(parse_table_information_schema(info), 3101009)

    def test_normal_multiple_create_options(self):
        info = """*************************** 1. row ***************************
AUTO_INCREMENT: 3101009
CREATE_OPTIONS: magical, partitioned"""
        self.assertEqual(parse_table_information_schema(info), 3101009)


class TestParsePartitionMap(unittest.TestCase):
    def test_single_partition(self):
        create_stmt = """*************************** 1. row ***************************
       Table: authz2
Create Table: CREATE TABLE `authz2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `identifierType` tinyint(4) NOT NULL,
  `identifierValue` varchar(255) NOT NULL,
  `registrationID` bigint(20) NOT NULL,
  `status` tinyint(4) NOT NULL,
  `expires` datetime NOT NULL,
  `challenges` tinyint(4) NOT NULL,
  `attempted` tinyint(4) DEFAULT NULL,
  `attemptedAt` datetime DEFAULT NULL,
  `token` binary(32) NOT NULL,
  `validationError` mediumblob DEFAULT NULL,
  `validationRecord` mediumblob DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `regID_expires_idx` (`registrationID`,`status`,`expires`),
  KEY `regID_identifier_status_expires_idx` (`registrationID`,`identifierType`,`identifierValue`,`status`,`expires`),
  KEY `expires_idx` (`expires`)
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (`id`)
(PARTITION `p_20201204` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)
"""
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "p_20201204")

    def test_two_partitions(self):
        create_stmt = """*************************** 1. row ***************************
       Table: authz2
Create Table: CREATE TABLE `authz2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `identifierType` tinyint(4) NOT NULL,
  `identifierValue` varchar(255) NOT NULL,
  `registrationID` bigint(20) NOT NULL,
  `status` tinyint(4) NOT NULL,
  `expires` datetime NOT NULL,
  `challenges` tinyint(4) NOT NULL,
  `attempted` tinyint(4) DEFAULT NULL,
  `attemptedAt` datetime DEFAULT NULL,
  `token` binary(32) NOT NULL,
  `validationError` mediumblob DEFAULT NULL,
  `validationRecord` mediumblob DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `regID_expires_idx` (`registrationID`,`status`,`expires`),
  KEY `regID_identifier_status_expires_idx` (`registrationID`,`identifierType`,`identifierValue`,`status`,`expires`),
  KEY `expires_idx` (`expires`)
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (`id`)
(PARTITION `before` VALUES LESS THAN (100),
PARTITION `p_20201204` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)
"""
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], ("before", "(100)"))
        self.assertEqual(results[1], "p_20201204")

    def test_mismatch_range_and_ai(self):
        create_stmt = """*************************** 1. row ***************************
       Table: authz2
Create Table: CREATE TABLE `authz2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `identifierType` tinyint(4) NOT NULL,
  `identifierValue` varchar(255) NOT NULL,
  `registrationID` bigint(20) NOT NULL,
  `status` tinyint(4) NOT NULL,
  `expires` datetime NOT NULL,
  `challenges` tinyint(4) NOT NULL,
  `attempted` tinyint(4) DEFAULT NULL,
  `attemptedAt` datetime DEFAULT NULL,
  `token` binary(32) NOT NULL,
  `validationError` mediumblob DEFAULT NULL,
  `validationRecord` mediumblob DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `regID_expires_idx` (`registrationID`,`status`,`expires`),
  KEY `regID_identifier_status_expires_idx` (`registrationID`,`identifierType`,`identifierValue`,`status`,`expires`),
  KEY `expires_idx` (`expires`)
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (`expires`)
(PARTITION `p_20201204` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)
"""
        with self.assertRaises(MismatchedIdException):
            parse_partition_map(create_stmt)


class TestSqlInput(unittest.TestCase):
    def test_escaping(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlInput("little bobby `;drop tables;")

    def test_whitespace(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlInput("my table")

    def test_okay(self):
        SqlInput("my_table")
        SqlInput("zz-table")


if __name__ == "__main__":
    unittest.main()
