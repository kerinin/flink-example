package com.datastax;

import java.time.LocalDateTime;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.typeinfo.Types;

public class App 
{
    public static void main(String[] args) throws Exception
        {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Build GameVictory table
        DataStream<Row> victories = env.fromElements(
          Row.of(LocalDateTime.parse("2021-08-21T13:02:30"), "Alice", 10),
          Row.of(LocalDateTime.parse("2021-08-21T13:03:58"), "Bob", 23),
          Row.of(LocalDateTime.parse("2021-08-21T13:04:25"), "Bob", 8),
          Row.of(LocalDateTime.parse("2021-08-21T13:05:05"), "Alice", 53),
          Row.of(LocalDateTime.parse("2021-08-21T13:10:01"), "Alice", 43))
          .returns(
            Types.ROW_NAMED(
                new String[] {"ts", "entity", "duration"},
                Types.LOCAL_DATE_TIME, Types.STRING, Types.INT));
        
        tableEnv.createTemporaryView(
          "GameVictory",
          victories,
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("entity", DataTypes.STRING())
              .column("duration", DataTypes.INT())
              .watermark("ts", "ts - INTERVAL '1' SECOND")
              .build());

        // Build GameDefeat table
        DataStream<Row> defeats = env.fromElements(
          Row.of(LocalDateTime.parse("2021-08-21T13:02:35"), "Bob", 3),
          Row.of(LocalDateTime.parse("2021-08-21T13:03:46"), "Bob", 8),
          Row.of(LocalDateTime.parse("2021-08-21T13:05:36"), "Alice", 2),
          Row.of(LocalDateTime.parse("2021-08-21T13:07:22"), "Bob", 7),
          Row.of(LocalDateTime.parse("2021-08-21T13:08:35"), "Alice", 5))
          .returns(
            Types.ROW_NAMED(
                new String[] {"ts", "entity", "duration"},
                Types.LOCAL_DATE_TIME, Types.STRING, Types.INT));
        
        tableEnv.createTemporaryView(
          "GameDefeat",
          defeats,
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("entity", DataTypes.STRING())
              .column("duration", DataTypes.INT())
              .watermark("ts", "ts - INTERVAL '1' SECOND")
              .build());

        // Build Purchase table
        DataStream<Row> purchases = env.fromElements(
          Row.of(LocalDateTime.parse("2021-08-21T13:01:02"), "Alice"),
          Row.of(LocalDateTime.parse("2021-08-21T13:03:51"), "Bob"))
          .returns(
            Types.ROW_NAMED(
                new String[] {"ts", "entity"},
                Types.LOCAL_DATE_TIME, Types.STRING));
        
        tableEnv.createTemporaryView(
          "Purchase",
          purchases,
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("entity", DataTypes.STRING())
              .watermark("ts", "ts - INTERVAL '1' SECOND")
              .build());

        Table resultTable = tableEnv.sqlQuery(
            "SELECT ts, entity FROM Purchase");

        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print();
        resultStream.writeAsText("output.txt", WriteMode.OVERWRITE);

        env.execute();
    }
}
