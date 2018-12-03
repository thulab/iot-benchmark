# reference:
# https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ttest_ind.html
# https://blog.csdn.net/m0_37777649/article/details/74938120
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from scipy import stats

result_file = 'LatestQueryTestAnalysis.csv'
# db = pymysql.connect(host='166.111.141.168', user='root', passwd='Ise_Nel_2017', port=3306, charset='utf8')
p_threshold=0.05
host='166.111.141.168'
port=3306
user='root'
passwd='Ise_Nel_2017'
charset='utf8'
database='auto_test'
test_group_size=18
insert_info_table_name='configInsertInfo'

def get_mysql_field(field, table):
    db = pymysql.connect(host=host, user=user, passwd=passwd, port=port, charset=charset)
    cur = db.cursor()
    cur.execute('use ' + database)
    # selectsql = 'select ' + field + ' from ' + table + ' limit 5'
    selectsql = 'select ' + field + ' from ' + table
    cur.execute(selectsql.encode('utf-8'))
    data = cur.fetchall()
    db.close()
    cost_time = []
    for item in data:
        cost_time.append(item[0])
    return np.array(cost_time)


def get_query_results():
    engine = create_engine('mysql+pymysql://'+user+':'+passwd+'@'+host+':'+str(port)+'/'+database)
    sql = 'select  avg, midAvg, totalTimes from queryResult;'
    data_df = pd.read_sql_query(sql, engine)
    sql = 'select projectID from queryResult;'
    projectID_df = pd.read_sql_query(sql, engine)
    sql = 'select version from ' + insert_info_table_name
    version_df = pd.read_sql_query(sql, engine)
    # print(df)
    # df = pd.DataFrame({'id':[1,2,3,4],'num':[12,34,56,89]})
    # df.to_sql('mydf', engine, index= False)
    return data_df, projectID_df, version_df


def t_test(field, new_table, baseline_table):
    cost_time1 = get_mysql_field(field, new_table)
    cost_time2 = get_mysql_field(field, baseline_table)
    # print(cost_time1)
    # print(cost_time2)
    new_std = np.std(cost_time1)
    baseline_std = np.std(cost_time2)
    std_diff_ratio = (baseline_std - new_std) / baseline_std

    levene_statistic, levene_pvalue = stats.levene(cost_time1, cost_time2)

    equal_variance = True
    if levene_pvalue < p_threshold:
        equal_variance = False
    ttest_statistic, ttest_pvalue = stats.ttest_ind(cost_time1, cost_time2, equal_var = equal_variance)
    stats.ttest_ind(cost_time1, cost_time2, equal_var = equal_variance)
    confidence = (1 - p_threshold)
    if ttest_pvalue > p_threshold:
        is_significant_difference = False
        # print("By double independent sample t-test analysis, "
        #       "the conclusion of auto-analysis system is that there is no significant difference "
        #       "between latest query average latency in " + table1 +
        #       " and baseline " + table2 +
        #       ", with " + str(confidence * 100) + "% confidence.")
    else:
        is_significant_difference = True
        # print("By double independent sample t-test analysis, "
        #       "the conclusion of auto-analysis system is that there is significant difference "
        #       "between latest query average latency in " + table1 +
        #       " and baseline " + table2 +
        #       ", with " + str(confidence * 100) + "% confidence.")
    return is_significant_difference, std_diff_ratio


def main():
    df, project_df, version_df = get_query_results()
    latest_version = str(version_df.iloc[-1, 0])
    latest_version.replace(' ', '_')
    project_baseline=project_df[0:test_group_size]
    project_new=project_df[-test_group_size:].reset_index(drop=True)
    t_test_results = []
    std_diffs = []
    for i in range(test_group_size):
        is_significant, std_diff_ratio = t_test('time', str(project_new.iloc[i, 0]), str(project_baseline.iloc[i, 0]))
        t_test_results.append(is_significant)
        std_diffs.append(std_diff_ratio)
    t_test_df = pd.DataFrame(t_test_results, columns=['sig'])
    std_diffs_df = pd.DataFrame(std_diffs, columns=['std'])
    # print(t_test_df)
    baseline_df = df[0:test_group_size].reset_index(drop=True)
    new_df = df[-test_group_size:].reset_index(drop=True)
    #  convert type: avg, midAvg, totalTimes
    baseline_df[['avg','midAvg', 'totalTimes']] = baseline_df[['avg', 'midAvg', 'totalTimes']].apply(pd.to_numeric)
    new_df[['avg','midAvg', 'totalTimes']] = new_df[['avg', 'midAvg', 'totalTimes']].apply(pd.to_numeric)
    # print(baseline_df)
    # print(new_df)
    diff_df = baseline_df - new_df
    # print(diff_df)
    diff_ratio_df = diff_df / baseline_df
    result_df = pd.concat([project_new, diff_ratio_df, std_diffs_df, t_test_df], axis=1)
    analysis_df = pd.DataFrame(result_df)
    print(analysis_df)
    analysis_df.to_csv(latest_version + '_' + result_file, index=False, float_format='%.4f')


if __name__ == '__main__':
    main()
