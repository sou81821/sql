#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import jaydebeapi
import pdb
import pandas.io.sql as psql


def check_column(df1, df2, args):
    for df1_column in df1.column.values:
        if df1_column in df2.column.values:
            # 定義の差分
            different = (df1[df1['column']==df1_column].values) != (df2[df2['column']==df1_column]).values
            # カラムの定義が異なる場合
            if False in different:
                print(df1_column, ' : {} の定義が異なる'.format(df1.columns[different[0]][0]))
        else:
            # カラムが存在しない場合
            print(df1_column, ' : {}にのみ存在'.format(args))


def main(args):
    # redshift接続
    driver = "com.amazon.redshift.jdbc42.Driver"
    host   = os.environ["HOST"] 
    redshift_id   = os.environ["REDSHIFT_ID"]
    redshift_pass = os.environ["REDSHIFT_PASS"]
    driver_path   = "RedshiftJDBC42-1.1.17.1017.jar"
    conn = jaydebeapi.connect(driver, host, [redshift_id, redshift_pass], driver_path)

    # SQL実行
    df1 = psql.read_sql("SELECT * FROM pg_table_def as table_dif WHERE table_dif.schemaname = 'common_raftel' AND table_dif.tablename = '{}' ORDER BY table_dif.column".format(args[1]), conn)
    df2 = psql.read_sql("SELECT * FROM pg_table_def as table_dif WHERE table_dif.schemaname = 'common_raftel' AND table_dif.tablename = '{}' ORDER BY table_dif.column".format(args[2]), conn)
    # curs.execute("SELECT * FROM pg_table_def as table_dif WHERE table_dif.schemaname = 'bi_smh' AND table_dif.tablename = 'smh_apy_kintone_dialog_history' ORDER BY table_dif.column")

    # カラムチェック
    check_column(df1, df2, args[1])
    check_column(df2, df1, args[2])

    conn.close()
    exit()

if __name__ == '__main__':
    args = sys.argv
    main(args)
