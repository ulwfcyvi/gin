package com.gin.clickhouse.java;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.ResultSet;

/**
 *
 * create table test(id UInt8 ,name String, age UInt8) engine = Memory;
 * insert into test values (1,'张三',18),(2,'李四',19),(3,'王五',20);
 *
 * @author jack
 * @date 2022/07/07
 */
public class ClickHouseJavaSingle {

    public static void main(String[] args) {
        try {
            ClickHouseProperties props = new ClickHouseProperties();
            props.setUser("");
            props.setPassword("");
            //连接配置 node01
            BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://172.21.21.205:8123/default", props);
            //获取连接
            ClickHouseConnection conn = dataSource.getConnection();
            //查询语句对象
            ClickHouseStatement statement = conn.createStatement();

            /*CREATE TABLE cs_user_info (
  `id` UInt64,
  `user_name` String,
  `pass_word` String,
  `phone` String,
  `email` String,
  `create_day` Date DEFAULT CAST(now(),'Date')
) ENGINE = MergeTree(create_day, intHash32(id), 8192);*/

            //插入
            //statement.execute("insert into cs_user_info values (5,'zl','35','13589093824','782377920@qq.com',now())");
            //查询数据
            ResultSet rs = statement.executeQuery("select id,user_name,pass_word from cs_user_info");
            while(rs.next()){
                int id = rs.getInt("id");
                String userName = rs.getString("user_name");
                String passWord = rs.getString("pass_word");
                System.out.println("id = "+id+",user_name = "+userName +",pass_word = "+passWord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
