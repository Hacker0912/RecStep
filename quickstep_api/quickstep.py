"""
Functionalities for communication between QuickStep and logic flow
"""
import collections
import sys
import subprocess
import json

config_json_file_name = 'Config.json'
with open(config_json_file_name) as config_json_file:
    config = json.load(config_json_file)
    print(config)

STATIC_DEBUG = config['Debug']['static_debug']
ANALYZER_OP = config['Optimization']['analyzer_level']
LOG_ON = config['Logging']['log']


class Database(object):

    @staticmethod
    def parse_query_result(result):
        parsed_by_line = [s.strip() for s in result.split('\n')]
        return parsed_by_line

    def __init__(self, quickstep_shell_dir):
        self.quickstep_shell_dir = quickstep_shell_dir
        self.query_counter = 0
        if LOG_ON:
            self.quickstep_query_execution_dag_log_file = open(config['Logging']['logging_directory'] + '/query_seq', 'w')

    def command_error_checking(self, output):
        parsed_by_line = self.parse_query_result(output)
        for line in parsed_by_line:
            tokens = [i.strip() for i in line.split()]
            for i in tokens:
                if i == 'ERROR:':
                    print(output)
                    raise Exception(output)

    def load_file_failure_checking(self, output):
        parsed_by_line = self.parse_query_result(output)
        for line in parsed_by_line:
            if line == '*** Check failure stack trace: ***':
                raise Exception(output)

    def sql_command(self, command):
        if STATIC_DEBUG:
            return 'DEBUG MODE'

        if LOG_ON:
            if len(command) < 8 or command[:8] != '\\analyze':
                self.quickstep_query_execution_dag_log_file.write('#####QUERY ID ' + str(self.query_counter) + '#####\n')
                self.query_counter += 1
                self.quickstep_query_execution_dag_log_file.write(command + '\n\n')

        command_str = self.quickstep_shell_dir + '/quickstep_client' + ' <<< ' + '\"' + command + '\"'
        output = subprocess.check_output(command_str, stderr=subprocess.STDOUT, shell=True, executable='/bin/bash')
        output = output.decode('utf-8')

        try:
            self.command_error_checking(output)
        except Exception as e:
            print(e)
            print('Exception caught when trying to execute the command:')
            print(command)
            self.stop()
            sys.exit(0)
        return output

    def analyze(self, table_list=[], count=False, range=False):
        # control the granularity of analytical queries for query optimization
        if ANALYZER_OP == 'full':
            count = False
            range = False
        if ANALYZER_OP == 'off':
            return

        # analyze the table names specified in the table list
        table_list_str = ''
        for table in table_list:
            table_list_str += ' ' + table

        if count and range:
            self.sql_command('\\analyzecount' + table_list_str + '\n')
            self.sql_command('\\analyzerange' + table_list_str + '\n')
        elif count and not range:
            self.sql_command('\\analyzecount' + table_list_str + '\n')
        elif not count and range:
            self.sql_command('\\analyzerange' + table_list_str + '\n')
        else:
            self.sql_command('\\analyze' + table_list_str + '\n')

    def stop(self):
        """
            Shut down the quickstep shell running in the background
        """
        self.sql_command('quit;')

    def create_table(self, table):
        attributes = table.attributes
        create_table_command = 'CREATE TABLE ' + table.table_name + ' ('

        for attribute_name in attributes:
            create_table_command += attribute_name + ' ' + attributes[attribute_name] + ', ';

        create_table_command = create_table_command[:len(create_table_command)-2]
        create_table_command += ')'
        create_table_command += ';'
        return self.sql_command(create_table_command)

    def drop_table(self, table_name):
        drop_table_command = 'DROP TABLE ' + table_name + ';'
        return self.sql_command(drop_table_command)

    def count_rows(self, table_name):
        if STATIC_DEBUG:
            return 0
        # count_command = 'COPY SELECT COUNT(*) FROM ' + table_name + ' TO stdout;';
        count_command = 'SELECT COUNT(*) FROM ' + table_name + ';';
        count_result = self.sql_command(count_command)
        parsed_result_by_line = self.parse_query_result(count_result)
        try:
            parsed_result_by_delimiter = [i.strip() for i in parsed_result_by_line[3].split('|')]
        except IndexError:
            print(count_result)
            self.stop()
            sys.exit(1)

        count_result_value = int(parsed_result_by_delimiter[1])
        return count_result_value

    def is_table_empty(self, table_name):
        if STATIC_DEBUG:
            return [False, 0]
        row_num = self.count_rows(table_name)
        return [row_num == 0, row_num]

    def output_data_from_table_to_csv(self, table_name, delimiter):
        output_file_name = table_name + '.csv'
        output_data_command = 'COPY ' + table_name + ' TO ' + '\'' + output_file_name + '\'' + \
                            ' WITH ' + ' (DELIMITER ' + '\'' + delimiter + '\'' + ');'
        self.sql_command(output_data_command)

    def load_data_from_file(self, table_name, file_name, delimiter):
        load_data_command = 'COPY ' + table_name + ' FROM ' + '\'' + file_name + '\'' + \
                            ' WITH ' + ' (DELIMITER ' + '\'' + delimiter + '\'' + ');'
        self.load_file_failure_checking(self.sql_command(load_data_command))

    def load_data_from_table(self, src_table, selected_src_table_attributes_names,
                             dest_table, dest_table_attributes_names, compute_intersection=True):
        """
        Insert data from a selected table (source table) into another existed table (target table) and
        perform deduplication on target table
        """
        src_table_attributes = src_table.attributes
        dest_table_attributes = dest_table.attributes
        # Validate the existence of attributes in both source table and target table
        for attribute in selected_src_table_attributes_names:
            if attribute not in src_table_attributes:
                raise Exception(attribute + ' is not in table ' + src_table.table_name)

        for attribute in dest_table_attributes_names:
            if attribute not in src_table_attributes:
                raise Exception(attribute + ' is not in table ' + dest_table.table_name)

        # Validate the equivalence of attribute number of selected attributes in source table
        # and attributes in target table
        if len(selected_src_table_attributes_names) != len(dest_table_attributes_names):
            raise Exception('The number of selected attributes is not equal to '
                            'the number of attributes in the target table')

        # Validate the data types of attributes selected in source table are the same as attributes in target table
        attributes_num = len(selected_src_table_attributes_names)
        for i in range(attributes_num):
            src_table_attribute = selected_src_table_attributes_names[i]
            src_table_attribute_type = src_table_attributes[src_table_attribute]
            dest_table_attribute = dest_table_attributes_names[i]
            dest_table_attribute_type = dest_table_attributes[dest_table_attribute]
            if src_table_attribute_type != dest_table_attribute_type:
                raise Exception('The data type of ' + src_table_attribute + ' in ' + src_table.table_name + ' is ' + src_table_attribute_type + \
                                ' but the data type of ' + dest_table_attribute + ' in ' + dest_table.table_name + \
                                ' is ' + dest_table_attribute_type)

        if compute_intersection:
            tmp_table = Table('TEMP_TABLE_INTERSECT')
            for attribute_name in dest_table_attributes:
                tmp_table.add_attribute(attribute_name, dest_table_attributes[attribute_name])
            self.create_table(tmp_table)

            compute_intersection_command = 'INSERT INTO TEMP_TABLE_INTERSECT' + \
                                           ' SELECT '
            compute_intersection_command_join_str = ''
            for i in range(attributes_num):
                src_table_attri_name = selected_src_table_attributes_names[i]
                dest_table_attri_name = dest_table_attributes_names[i]
                compute_intersection_command += 'src.' + src_table_attri_name + ' AS ' + \
                                                dest_table_attri_name + ', '
                compute_intersection_command_join_str += 'src.' + src_table_attri_name + ' = ' + \
                                                         'dest.' + dest_table_attri_name + ' AND '
            compute_intersection_command_join_str = \
                compute_intersection_command_join_str[:len(compute_intersection_command_join_str)-5]

            compute_intersection_command = compute_intersection_command[:len(compute_intersection_command)-2]
            compute_intersection_command += ' FROM ' + src_table.table_name + ' src, ' + \
                                             dest_table.table_name + ' dest'
            compute_intersection_command += ' WHERE ' + compute_intersection_command_join_str + ';'

            self.sql_command(compute_intersection_command)
            self.analyze(['TEMP_TABLE_INTERSECT'], count=True)

            # compute the set-difference
            tmp_table = Table('TEMP_TEMP_TABLE_SET_DIFF')
            for attribute_name in dest_table_attributes:
                tmp_table.add_attribute(attribute_name, dest_table_attributes[attribute_name])

            compute_set_diff_command = 'INSERT INTO TEMP_TEMP_TABLE_SET_DIFF' + \
                                       ' SELECT * FROM ' + src_table.table_name + ' t1' +\
                                       ' WHERE NOT EXISTS (SELECT * FROM TEMP_TABLE_INTERSECT t2 WHERE '
            compute_set_diff_group_by_command = ' GROUP BY '
            for i in range(attributes_num):
                compute_set_diff_command += 't2.' + dest_table_attributes_names[i] + ' = ' + \
                                            't1.' + selected_src_table_attributes_names[i] + ' AND '
                compute_set_diff_group_by_command += dest_table_attributes_names[i] + ', '

            compute_set_diff_command = compute_set_diff_command[:len(compute_set_diff_command)-5] + ')' + ';'
            compute_set_diff_dedup_command = 'INSERT INTO TEMP_TABLE_SET_DIFF SELECT * FROM ' + \
                                             'TEMP_TEMP_TABLE_SET_DIFF' + \
                                             compute_set_diff_group_by_command[:len(compute_set_diff_group_by_command)-2]

            compute_set_diff_dedup_command += ';'
            self.create_table(tmp_table)
            self.sql_command(compute_set_diff_command)
            self.analyze(['TEMP_TEMP_TABLE_SET_DIFF'], count=True)
            self.drop_table('TEMP_TABLE_INTERSECT')
            tmp_table.rename('TEMP_TABLE_SET_DIFF')
            self.create_table(tmp_table)
            self.sql_command(compute_set_diff_dedup_command)
            self.drop_table('TEMP_TEMP_TABLE_SET_DIFF')
            self.analyze(['TEMP_TABLE_SET_DIFF'], count=True)
            insert_set_diff_into_dest_table_command = 'INSERT INTO ' + dest_table.table_name + \
                                                      ' SELECT * FROM TEMP_TABLE_SET_DIFF;'
            self.sql_command(insert_set_diff_into_dest_table_command)
            self.drop_table('TEMP_TABLE_SET_DIFF')
        else:
            tmp_table = Table('TEMP_TABLE_WITHOUT_DEDUP')
            for attribute_name in dest_table_attributes:
                tmp_table.add_attribute(attribute_name, dest_table_attributes[attribute_name])
            self.create_table(tmp_table)
            self.sql_command('INSERT INTO TEMP_TABLE_WITHOUT_DEDUP ' + 'SELECT * FROM ' + src_table.table_name + ';')
            self.sql_command('INSERT INTO TEMP_TABLE_WITHOUT_DEDUP ' + 'SELECT * FROM ' + dest_table.table_name + ';')
            self.analyze(['TEMP_TABLE_WITHOUT_DEDUP'], count=True)
            self.drop_table(dest_table.table_name)
            dedup_command = 'SELECT * FROM TEMP_TABLE_WITHOUT_DEDUP GROUP BY '
            for i in range(attributes_num):
                dedup_command += dest_table.table_name + '.' + dest_table_attributes_names[i] + ', '
            dedup_command = dedup_command[:len(dedup_command)-2]
            self.sql_command('INSERT INTO ' + dest_table.table_name + ' ' + dedup_command + ';')
            self.drop_table(tmp_table.table_name)

    def dedup_table(self, src_table):
        src_table_attributes = src_table.attributes
        tmp_table = Table('TEMP_TABLE')
        src_table_attribute_names = list(src_table_attributes.keys())
        for attribute_name in src_table_attributes:
            tmp_table.add_attribute(attribute_name, src_table_attributes[attribute_name])
        attributes_num = len(src_table_attributes)
        self.create_table(tmp_table)
        self.sql_command('INSERT INTO TEMP_TABLE ' + 'SELECT * FROM ' + src_table.table_name + ';')
        self.drop_table(src_table.table_name)
        self.analyze(['TEMP_TABLE'], count=True)
        dedup_command = 'SELECT * FROM TEMP_TABLE GROUP BY '
        for i in range(attributes_num):
            dedup_command +=  'TEMP_TABLE.' + src_table_attribute_names[i] + ', '
        dedup_command = dedup_command[:len(dedup_command) - 2]
        self.create_table(src_table)
        self.sql_command('INSERT INTO ' + src_table.table_name + ' ' + dedup_command + ';')
        self.drop_table(tmp_table.table_name)


class Table(object):

    def __init__(self, table_name):
        self.table_name = table_name
        self.attributes = collections.OrderedDict()
        self.attribute_num = 0

    def rename(self, new_table_name):
        self.table_name = new_table_name

    def add_attribute(self, attribute_name, attribute_type):
        if attribute_name in self.attributes:
            raise Exception(attribute_name + ' already exists in table ' + self.table_name)
        else:
            self.attributes[attribute_name] = attribute_type
            self.attribute_num += 1
