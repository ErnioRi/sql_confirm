import cursor as cursor
import pymysql
import time
import sys

def sql_confirm(sql, in_cont = "300", in_terminal = "1", in_show_group = "20"):
    # 连接database
    cont = int(in_cont)
    terminal = float(in_terminal)
    show_group = int(in_show_group)
    conn = pymysql.connect(
        host="10.118.71.101",
        port=3306,
        user="mdm", password="mdm123456",
        database="vehicle_vw",
        charset='utf8')

    # 得到一个可以执行SQL语句的光标对象
    cursor = conn.cursor()  # 执行完毕返回的结果集默认以元组显示
    # 得到一个可以执行SQL语句并且将结果作为字典返回的游标
    # cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    # 定义要执行的SQL语句
    '''= "SELECT DISTINCT  oe2modelcatalog.epc_model_id,oe2modelcatalog.diagram_code,oe2diagram.image_label," \
      "oe2modelcatalog.oe_num,oe2diagram.std_name,oe2modelcatalog.seq_num,oe.oe_id," \
      "vehicle_oe.bmw_oe.price as price_4s,oe.oe_code,oe.oe_name,oe2diagram.sa_code1," \
      "oe2diagram.sa_code2,oe2diagram.steer_trans as oe_steer_trans,oe2diagram.sa_inclu," \
      "oe2diagram.start_date as oe_start_date,oe2diagram.end_date as oe_end_date,oe2diagram.type,oe.info " \
      " FROM oe2modelcatalog " \
      "LEFT JOIN oe2diagram ON oe2diagram.diagram_code = oe2modelcatalog.diagram_code and oe2diagram.seq_num = oe2modelcatalog.seq_num " \
      "LEFT JOIN oe ON oe.oe_id = oe2diagram.oe_id LEFT JOIN vehicle_oe.bmw_oe on vehicle_oe.bmw_oe.oe_code = oe.oe_code_trim " \
      "WHERE oe.oe_code_trim = '84530002033' ORDER BY oe2diagram.image_label desc, oe2modelcatalog.diagram_code limit 5;"'''

    # 执行SQL语句
    res_len = cursor.execute(sql)

    info_dict = dict()
    diff_flag = True
    # 加载样例
    for j in range(0, res_len):
        cursor.scroll(j, mode="absolute")
        info_dict[j] = list(cursor.fetchone())
    time.sleep(1)

    for i in range(0, cont):
        diff_flag = False
        if cursor.execute(sql) != res_len:
            print("different data number in query %d\n", i)
            continue

        err_cont = 0
        for j in range(0, res_len):
            cursor.scroll(j, mode="absolute")
            res_cursor = list(cursor.fetchone())
            if info_dict[j] != res_cursor:


                err_cont += 1
                if err_cont > 5:
                    continue
                print("different in query %d, line %d" % (i, j))
                print("cursor content: ", res_cursor)
                print("dict content: ", info_dict[j])
                diff_flag = True

        if (not diff_flag) & (i%show_group == 0):
            print("query %d is ok" % i)
        elif diff_flag:
            print("changing dictionary...")
            for j in range(0, res_len):
                cursor.scroll(j, mode="absolute")
                info_dict[j] = list(cursor.fetchone())

        time.sleep(terminal)

    # 关闭光标对象
    cursor.close()

    # 关闭数据库连接
    conn.close()

if __name__ == "__main__":
    sql_confirm(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])