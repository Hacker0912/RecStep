"""
Functionalities for communication between QuickStep and logic flow
"""
import collections
import sys
import subprocess

from copy import deepcopy
from execution.config import *


class QuickStep(object):
    @staticmethod
    def parse_query_result(result):
        parsed_by_line = [s.strip() for s in result.split("\n")]
        return parsed_by_line

    def __init__(self):
        self.__quickstep_shell_dir = QUICKSTEP_SHELL_DIR
        self.__query_counter = 0
        if LOG_ON:
            self.query_execution_dag_log_file = open(
                "{}/query_seq".format(LOG_DIR), "w"
            )

    def command_error_checking(self, output):
        parsed_by_line = self.parse_query_result(output)
        for line in parsed_by_line:
            tokens = [i.strip() for i in line.split()]
            for i in tokens:
                if i == "ERROR:":
                    print(output)
                    raise Exception(output)

    def load_file_failure_checking(self, output):
        parsed_by_line = self.parse_query_result(output)
        for line in parsed_by_line:
            if line == "*** Check failure stack trace: ***":
                raise Exception(output)

    def sql_command(self, command):
        if STATIC_DEBUG:
            return "DEBUG MODE"

        if LOG_ON:
            self.query_execution_dag_log_file.write(
                "#####QUERY ID {} #####\n".format(self.__query_counter)
            )
            self.__query_counter += 1
            self.query_execution_dag_log_file.write("{}\n\n".format(command))

        command_str = '{}/quickstep_client <<< "{}"'.format(
            self.__quickstep_shell_dir, command
        )
        output = subprocess.check_output(
            command_str, stderr=subprocess.STDOUT, shell=True, executable="/bin/bash"
        )
        output = output.decode("utf-8")

        try:
            self.command_error_checking(output)
        except Exception as e:
            print(e)
            print("Exception caught when trying to execute the command:")
            print(command)
            self.stop()
            sys.exit(0)
        return output

    def analyze(
        self,
        table_list=[],
        count=False,
        range_analyze=False,
        analyze_all_tables=ANALYZE_ALL_TABLES,
    ):
        # control the granularity of analytical queries for query optimization
        if ANALYZER_OP == "off":
            return

        # analyze the table names specified in the table list
        table_list_str = ""
        if not analyze_all_tables:
            table_list_str = " ".join(table_list)

        if ANALYZER_OP == "full":
            self.sql_command("\\analyze {}\n".format(table_list_str))
            return

        if count:
            self.sql_command("\\analyzecount {}\n".format(table_list_str))
        if range_analyze:
            self.sql_command("\\analyzerange {}\n".format(table_list_str))

    def stop(self):
        """
        Shut down the quickstep shell running in the background
        """
        self.sql_command("quit;")

    def create_table(self, table):
        attribute_name_type_pairs = [
            "{} {}".format(attribute, table.attributes[attribute])
            for attribute in table.attributes
        ]
        attribute_name_type_pairs_str = ",".join(attribute_name_type_pairs)
        create_table_command = "CREATE TABLE {} ({});".format(
            table.table_name, attribute_name_type_pairs_str
        )
        return self.sql_command(create_table_command)

    def drop_table(self, table_name):
        drop_table_command = "DROP TABLE " + table_name + ";"
        return self.sql_command(drop_table_command)

    def count_rows(self, table_name):
        if STATIC_DEBUG:
            return 0
        count_command = "SELECT COUNT(*) FROM {};".format(table_name)
        count_result = self.sql_command(count_command)
        parsed_result_by_line = self.parse_query_result(count_result)
        try:
            parsed_result_by_delimiter = [
                i.strip() for i in parsed_result_by_line[3].split("|")
            ]
        except IndexError:
            print(count_result)
            self.stop()
            sys.exit(1)

        count_result_value = int(parsed_result_by_delimiter[1])
        return count_result_value

    def output_data_from_table_to_csv(self, table_name, delimiter):
        output_file_name = "{}.csv".format(table_name)
        output_data_command = "COPY {} TO '{}' WITH (DELIMETER '{}');".format(
            table_name, output_file_name, delimiter
        )
        self.sql_command(output_data_command)

    def load_data_from_file(self, table_name, file_name, delimiter):
        load_data_command = "COPY {} FROM '{}' WITH (DELIMITER '{}');".format(
            table_name, file_name, delimiter
        )
        self.load_file_failure_checking(self.sql_command(load_data_command))

    def load_data_from_table(
        self,
        src_table,
        dest_table,
        compute_intersection="compute_intersection",
        deduplication=True,
    ):
        """
        Insert data from a selected table (source table) into another existed table (target table) and
        perform deduplication on target table or the insertion part only (assuming src table has been pre-deduplicated)
            - source table and target table must have the same schema
        """
        src_table_attribute_names = [attribute for attribute in src_table.attributes]
        dest_table_attribute_names = [attribute for attribute in dest_table.attributes]
        attribute_num = len(src_table.attributes)
        tmp_table = deepcopy(dest_table)
        if not deduplication:
            # insert all tuples from the source table directly into the target table
            insert_command = "INSERT INTO {} SELECT * FROM {}".format(
                dest_table.table_name, src_table.table_name
            )
            self.sql_command(insert_command)
            return

        if compute_intersection:
            # compute the intersection
            tmp_table.rename("TEMP_TABLE_INTERSECT")
            self.create_table(tmp_table)
            projection_attributes = list()
            join_strs = list()
            for i in range(attribute_num):
                src_table_attribute_name = src_table_attribute_names[i]
                dest_table_attribute_name = dest_table_attribute_names[i]
                projection_attributes.append(
                    "t1.{} AS {}".format(
                        src_table_attribute_name, dest_table_attribute_name
                    )
                )
                join_strs.append(
                    "t1.{} = t2.{}".format(
                        src_table_attribute_name, dest_table_attribute_name
                    )
                )
            projection_str = ", ".join(projection_attributes)
            join_str = " AND ".join(join_strs)
            compute_intersection_command = """INSERT INTO TEMP_TABLE_INTERSECT 
                SELECT {} FROM {} t1, {} t2 WHERE {};
            """.format(
                projection_str, src_table.table_name, dest_table.table_name, join_str
            )
            self.sql_command(compute_intersection_command)
            self.analyze(["TEMP_TABLE_INTERSECT"], count=True)
            # compute the set-difference
            tmp_table.rename("TEMP_TABLE_SET_DIFF")
            self.create_table(tmp_table)
            compute_set_diff_command = """INSERT INTO TEMP_TABLE_SET_DIFF 
                SELECT * FROM {} t1 WHERE NOT EXISTS 
                    (SELECT * FROM TEMP_TABLE_INTERSECT t2 WHERE {});
            """.format(
                src_table.table_name, join_str
            )
            self.sql_command(compute_set_diff_command)
            self.analyze(["TEMP_TABLE_SET_DIFF"], count=True)
            self.drop_table("TEMP_TABLE_INTERSECT")
            # dedup the set-difference
            tmp_table.rename("TEMP_TABLE_SET_DIFF_DEDUP")
            self.create_table(tmp_table)
            tmp_table.rename("TEMP_TABLE_SET_DIFF")
            self.dedup_table(tmp_table, dest_table_name="TEMP_TABLE_SET_DIFF_DEDUP")
            # insert the results from set-difference into the target table
            tmp_table.rename("TEMP_TABLE_SET_DIFF_DEDUP")
            self.load_data_from_table(tmp_table, dest_table, deduplication=False)
            self.drop_table("TEMP_TABLE_SET_DIFF_DEDUP")
        else:
            # insert tuples from both tables into new one without deduplication
            tmp_table.rename("TEMP_TABLE_WITHOUT_DEDUP")
            self.create_table(tmp_table)
            self.analyze(["TEMP_TABLE_WITHOUT_DEDUP"], count=True)
            insert_union_all_command = """ INSERT TO TEMP_TABLE_WITHOUT_DEDUP SELECT * 
                FROM (SELECT * FROM {} UNION ALL SELECT * FROM {}) t;
            """.format(
                src_table.table_name, dest_table.table_name
            )
            self.sql_command(insert_union_all_command)
            self.analyze(["TEMP_TABLE_WITHOUT_DEDUP"], count=True)
            self.drop_table(dest_table.table_name)
            # deduplication
            self.create_table(dest_table)
            self.analyze([dest_table.table_name], count=True)
            self.dedup_table(tmp_table, dest_table_name=dest_table.table_name)

    def dedup_table(self, src_table, dest_table_name=None):
        src_table_attribute_names = [attribute for attribute in src_table.attributes]
        attributes_num = len(src_table.attributes)
        group_by_str = ",".join(
            [src_table_attribute_names[i] for i in range(attributes_num)]
        )
        # store the deduplicated results in the table of the same name
        if dest_table_name is None:
            tmp_table = deepcopy(src_table)
            tmp_table.rename("TEMP_TABLE", count=True)
            # insert tuples from the source table into temporary table without deduplication
            self.create_table(tmp_table)
            self.load_data_from_table(src_table, tmp_table, deduplication=False)
            self.drop_table(src_table.table_name)
            self.analyze(["TEMP_TABLE"], count=True)
            # perform deduplication and insert the deduplicated tuples back into the source table
            self.create_table(src_table)
            dedup_command = (
                "INSERT INTO {} SELECT * FROM TEMP_TABLE GROUP BY {};".format(
                    src_table.table_name, group_by_str
                )
            )
            self.sql_command(dedup_command)
            drop_table = tmp_table
        else:
            self.analyze([src_table.table_name], count=True)
            dedup_command = "INSERT INTO {} SELECT * FROM {} GROUP BY {};".format(
                dest_table_name, src_table.table_name, group_by_str
            )
            drop_table = src_table
        self.sql_command(dedup_command)
        self.drop_table(drop_table.table_name)


class Table(object):
    def __init__(self, table_name):
        self.table_name = table_name
        self.attributes = collections.OrderedDict()
        self.attribute_num = 0

    def rename(self, new_table_name):
        self.table_name = new_table_name

    def add_attribute(self, attribute_name: str, attribute_type: str):
        if attribute_name in self.attributes:
            raise Exception(
                "{} already exists in table {}".format(attribute_name, self.table_name)
            )
        else:
            self.attributes[attribute_name] = attribute_type
            self.attribute_num += 1
