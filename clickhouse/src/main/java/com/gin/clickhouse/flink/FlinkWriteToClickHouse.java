package com.gin.clickhouse.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author jack
 * @date 2022/07/07
 */
public class FlinkWriteToClickHouse {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));*/

        // yum install nc -y
        // yum install nmap -y
        // netstat -natp |grep 8888
        // nc -lk 8888
        // 6,zl,pw,phone,email
        // 7,wb,pw,phone,email

        // /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

        // brew install nc
        // brew install nmap
        // netstat -natp |grep 8888
        // nc -lk 8888
        // 6,zl,26
        // 7,wb,26

        DataStreamSource<String> dss = env.socketTextStream("localhost", 8888, "\n");

        SingleOutputStreamOperator<Tuple5<Integer, String, String ,String ,String>> result = dss
                .map((MapFunction<String, Tuple5<Integer, String, String ,String ,String>>) value -> {
                    String[] split = value.split(",");
                    return new Tuple5<>(Integer.parseInt(split[0]), split[1], split[2], split[3], split[4]);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple5<Integer, String, String ,String ,String>>() {
                }));

        //String insetIntoCkSql = "insert into test (id,name,age) values (?,?,?)";
        //String insetIntoCkSql = "insert into cs_user_info values (?,?,?,?,?,now())";
        String insetIntoCkSql = "insert into cs_user_info values (?,?,?,?,?,?)";

        SinkFunction<Tuple5<Integer, String, String ,String ,String>> sink = JdbcSink.sink(
                // sql 语句
                insetIntoCkSql,
                // 配置从 DataStream 中如何获取数据, 来填补sql语句中的 "?"
                new JdbcStatementBuilder<Tuple5<Integer, String, String ,String ,String>>() {
                    @Override
                    public void accept(PreparedStatement statement, Tuple5<Integer, String, String ,String ,String > value) throws SQLException {
                        statement.setInt(1, value.f0);
                        statement.setString(2, value.f1);
                        statement.setString(3, value.f2);
                        statement.setString(4, value.f3);
                        statement.setString(5, value.f4);
                        statement.setDate(6, new Date(System.currentTimeMillis()));
                    }
                },
                //执行sql语句配置(没达到设置的批数执行一次)
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://172.0.0.0:8123/default")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("")
                        .withPassword("")
                        .build()

        );

        result.print();
        result.addSink(sink);

        try {
            env.execute("flink clickhouse sink");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
