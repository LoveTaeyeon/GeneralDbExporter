#!/usr/bin/python
# encoding=utf8
import os

from uuid import UUID
import json
import time
from six.moves.cPickle import loads

GENERAL_EXPORT_QUERY_STEP_SIZE = 10000
EXPORT_TYPE_JSON = 1
EXPORT_TYPE_CSV = 2
DAY_TIME = 60 * 60 * 24
UUID_KEYWORD_LIST = ['uid', 'userid', 'sticker_id']


def pickle_loads_w_none(s, default=None):
    if s:
        return loads(s)
    return default


def camel_to_underline(string):
    result = ''
    if isinstance(string, str):
        for char in string:
            result += char if char.islower() else '_' + char.lower()
    return result


def underline_to_camel(string):
    result = ''
    if isinstance(string, str):
        chat_arr = string.split('_')
        for i in range(len(chat_arr)):
            if i == 0:
                result += chat_arr[i]
            else:
                result += chat_arr[i].capitalize()
    if len(result) > 0 and result[0].isupper():
        result = result[0].lower() + result[1:]
    return result


def to_dict(row, description):
    result = {}
    for i in range(len(description)):
        column_name = description[i][0]
        # mysql type binary
        if description[i][1] == 254:
            result[underline_to_camel(column_name)] = str(UUID(bytes=row[i]))
        # mysql type blob
        elif description[i][1] == 252:
            if row[i] and ((row[i][0] == '[' and row[i][-1] == ']') or (row[i][0] == '{' and row[i][-1] == '}')):
                try:
                    result[underline_to_camel(column_name)] = json.loads(row[i])
                except:
                    pass
            else:
                if row[i] and isinstance(row[i], str) and row[i][0] == '\x80':
                    try:
                        result[underline_to_camel(column_name)] = pickle_loads_w_none(row[i])
                    except:
                        pass
        else:
            result[underline_to_camel(column_name)] = row[i]
    return result


def generator_query_colunm_sql(query_columns):
    sql = ""
    for i in range(len(query_columns)):
        if i != 0:
            sql += ","
        sql += query_columns[i]
    return sql


def generator_query_where_sql(value_list):
    sql = " where "
    for i in range(len(value_list)):
        if i != 0:
            sql += ' and '
        sql += value_list[i]
    return sql


def generator_count_sql(table_name, value_list):
    sql = "select count(*) from " + table_name
    if value_list is not None and len(value_list) > 0:
        sql += generator_query_where_sql(value_list)
    return sql


def get_count_number(conn, count_sql):
    cursor = conn.cursor()
    cursor.execute(count_sql)
    return cursor.fetchone()[0]


def generator_index_column_sql(index_columns):
    line = ""
    for i in range(len(index_columns)):
        if i != 0:
            line += ","
        line += index_columns[i]
    return line


def generator_data_query_sql(table_name, query_columns, value_list, index_columns, is_limit=True):
    sql = "select " + generator_query_colunm_sql(query_columns)
    sql += " from " + table_name
    if value_list is not None and len(value_list) > 0:
        sql += generator_query_where_sql(value_list)
    sql += " order by " + generator_index_column_sql(index_columns) + " asc"
    if is_limit:
        sql += " limit " + str(GENERAL_EXPORT_QUERY_STEP_SIZE)
    return sql


def get_query_data(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    description = cursor.description
    result_data = []
    for row in rows:
        try:
            result_data.append(to_dict(row, description))
        except Exception as e:
            print(e)
    return result_data


def create_file_path(local_file_path):
    temp_arr = local_file_path.split("/")
    file_name = temp_arr[len(temp_arr) - 1]
    dir_path = local_file_path.replace("/" + file_name, "")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def get_file_info(table_name, export_type):
    file_path = news_setting.GENERAL_EXPORT_TEMP_FILE_PATH + "/" + table_name
    if export_type == EXPORT_TYPE_CSV:
        file_path += ".csv"
    elif export_type == EXPORT_TYPE_JSON:
        file_path += ".json"
    create_file_path(file_path)
    return file_path


def to_string(row):
    if isinstance(row, unicode):
        return str(row.encode("UTF-8"))
    else:
        return str(row)


def get_same_index_data_key(query_columns, row):
    key = ""
    for column_index in range(len(query_columns)):
        if column_index != 0:
            key += "-"
        key += to_string(row.get(underline_to_camel(query_columns[column_index])))
    return key


def get_same_index_data_dict(query_data, query_columns, index_columns):
    same_index_value_dict = {}
    data_size = len(query_data)
    if data_size < 2:
        return {}
    last_data = query_data[data_size - 1]
    last_data_key = get_same_index_data_key(query_columns, last_data)
    same_index_value_dict[last_data_key] = True
    last_index_value = {}
    for index_column in index_columns:
        last_index_value[index_column] = last_data.get(underline_to_camel(index_column))
    i = 2
    while i <= data_size:
        row = query_data[data_size - i]
        is_same = True
        for index_column in index_columns:
            if row.get(index_column) != last_index_value.get(index_column):
                is_same = False
                break
        if is_same:
            key = get_same_index_data_key(query_columns, row)
            same_index_value_dict[key] = True
        else:
            break
        i += 1
    return same_index_value_dict


def translate_param_value(param_value, index_column):
    if index_column in UUID_KEYWORD_LIST:
        param_value = str(UUID(param_value).bytes)
        if param_value.find("'") != -1:
            param_value = param_value.replace("'", "\\'")
        if param_value.find('"') != -1:
            param_value = param_value.replace('"', '\\"')
        param_value = "'" + param_value + "'"
    return to_string(param_value)


def generator_index_value_list(index_columns, step_data, option='>'):
    value_temp_list = []
    if len(index_columns) > 2:
        raise Exception("General export util not support more than 2 index now !")
    if len(index_columns) == 2:
        if option.replace(" ", "") == '>=':
            result_value_request = "("
            result_value_request += "(" + index_columns[0] + " = " + translate_param_value(step_data[underline_to_camel(index_columns[0])], index_columns[0]) + " and "
            result_value_request += index_columns[1] + " > " + translate_param_value(step_data[underline_to_camel(index_columns[1])], index_columns[1]) + ")"
            result_value_request += " or " + index_columns[0] + " > " + translate_param_value(step_data[underline_to_camel(index_columns[0])], index_columns[0])
            result_value_request += ")"
            value_temp_list.append(result_value_request)
        elif option.replace(" ", "") == '=':
            result_value_request = "(" + index_columns[0] + " = " + translate_param_value(step_data[underline_to_camel(index_columns[0])], index_columns[0]) + " and "
            result_value_request += index_columns[1] + " = " + translate_param_value(step_data[underline_to_camel(index_columns[1])], index_columns[1]) + ")"
            value_temp_list.append(result_value_request)
        return value_temp_list
    if step_data != None:
        for index_column in index_columns:
            step_value = step_data[underline_to_camel(index_column)]
            step_value = translate_param_value(step_value, index_column)
            value_temp_list.append(index_column + option + to_string(step_value))
    return value_temp_list


def complete_same_value_data(conn, table_name, query_columns, value_list, index_columns, query_data, file, export_type):
    value_temp_list = []
    same_index_value_dict = get_same_index_data_dict(query_data, query_columns, index_columns)
    if value_list is not None and len(value_list) > 0:
        value_temp_list.extend(value_list)
    step_data = query_data[len(query_data) - 1]
    value_temp_list.extend(generator_index_value_list(index_columns, step_data, option=" = "))
    complete_data_sql = generator_data_query_sql(table_name, query_columns, value_temp_list, index_columns, is_limit=False)
    print "Complete sql :", complete_data_sql
    complete_datas = get_query_data(conn, complete_data_sql)
    complete_lines = ""
    count = 0
    for complete_data in complete_datas:
        key = get_same_index_data_key(query_columns, complete_data)
        if same_index_value_dict.get(key) == None:
            complete_lines += translate_row(export_type, complete_data, query_columns) + "\n"
            count += 1
    if len(complete_lines) != 0:
        file.writelines(complete_lines)
    print(str(count) + " rows is complete to file")
    return count


def translate_row(export_type, row, query_columns):
    if export_type == EXPORT_TYPE_JSON:
        return to_string(json.dumps(row))
    else:
        line = ""
        for i in range(len(query_columns)):
            if i != 0:
                line += ","
            line += to_string(row.get(underline_to_camel(query_columns[i])))
        return line


def judge_is_endless_loop(last_step_value, this_last_step_value, index_columns):
    if last_step_value is None:
        return False
    if len(index_columns) == 1:
        if last_step_value[underline_to_camel(index_columns[0])] != this_last_step_value[underline_to_camel(index_columns[0])]:
            return False
    else:
        for index_column in index_columns:
            if last_step_value[underline_to_camel(index_column)] != this_last_step_value[underline_to_camel(index_column)]:
                return False
    return True


def write_csv_title(file, query_columns):
    line = ''
    for i in range(len(query_columns)):
        if i != 0:
            line += ','
        line += '"' + query_columns[i] + '"'
    line += "\n"
    file.writelines(line)


def get_general_data(conn, table_name, query_columns, index_columns, value_list, bucket_name, export_path, export_type=EXPORT_TYPE_JSON, limit_number=None):
    begin = time.time()
    file_path = get_file_info(table_name, export_type)
    with open(file_path, 'w+') as file:
        if export_type == EXPORT_TYPE_CSV:
            write_csv_title(file, query_columns)
        count_sql = generator_count_sql(table_name, value_list)
        total_number = get_count_number(conn, count_sql)
        value_request = None
        total_export_number = 0
        last_step_value = None
        while True:
            line = ""
            value_temp_list = []
            if value_list is not None and len(value_list) > 0:
                value_temp_list.extend(value_list)
            if value_request is not None:
                value_temp_list.extend(value_request)
            query_sql = generator_data_query_sql(table_name, query_columns, value_temp_list, index_columns)
            print "Select sql:", query_sql
            query_data = get_query_data(conn, query_sql)
            query_data_len = len(query_data)
            total_export_number += query_data_len
            for row in query_data:
                line += translate_row(export_type, row, query_columns) + "\n"
            file.writelines(line)
            if len(query_data) < GENERAL_EXPORT_QUERY_STEP_SIZE:
                break
            else:
                step_data = query_data[len(query_data) - 1]
                if judge_is_endless_loop(last_step_value, step_data, index_columns):
                    print("=======================================================")
                    print "====== [Error] Is endless loop,thread shutdown ! ======"
                    print("=======================================================")
                    break
                else:
                    last_step_value = step_data
                option = " > "
                if len(index_columns) == 2:
                    option = " >= "
                value_request = generator_index_value_list(index_columns, step_data, option=option)
                # next step will skip this index_value , so we should complete this value
                complete_count = complete_same_value_data(conn, table_name, query_columns, value_list, index_columns, query_data, file, export_type)
                total_export_number += complete_count
                print str(total_export_number) + " rows is finished write to file"
            if limit_number is not None and limit_number < total_export_number:
                break
        file_name = table_name
        if len(export_path) != 0:
            file_name = export_path + table_name
        if export_type == EXPORT_TYPE_CSV:
            file_name += ".csv"
        elif export_type == EXPORT_TYPE_JSON:
            file_name += ".json"
    end = time.time()
    print("==============  Export Schedule Finished ==============")
    print("=======================================================")
    print("Total time cost:" + str(end - begin)
          + "\nTotal export number is :" + str(total_export_number)
          + "\nReal query and calculate time is :" + str(end - begin))
