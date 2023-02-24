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

/*
 * 
 * Notes
 * 
 * - The main method caused an error: Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF' left table's time attribute field 
 *   IE - we can't join on arbitrary fields, for example a value shifted forward in time
 */
public class App 
{
    public static void main(String[] args) throws Exception
        {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Build GameVictory table
        DataStream<Row> victories = env.fromElements(
          Row.of(LocalDateTime.parse("2021-08-21T13:02:30"), "Alice", 10, true),
          Row.of(LocalDateTime.parse("2021-08-21T13:02:35"), "Bob", 3, false),
          Row.of(LocalDateTime.parse("2021-08-21T13:03:46"), "Bob", 8, false),
          Row.of(LocalDateTime.parse("2021-08-21T13:03:58"), "Bob", 23, true),
          Row.of(LocalDateTime.parse("2021-08-21T13:04:25"), "Bob", 8, true),
          Row.of(LocalDateTime.parse("2021-08-21T13:05:05"), "Alice", 53, true),
          Row.of(LocalDateTime.parse("2021-08-21T13:05:36"), "Alice", 2, false),
          Row.of(LocalDateTime.parse("2021-08-21T13:07:22"), "Bob", 7, false),
          Row.of(LocalDateTime.parse("2021-08-21T13:08:35"), "Alice", 5, false),
          Row.of(LocalDateTime.parse("2021-08-21T13:10:01"), "Alice", 43, true))
          .returns(
            Types.ROW_NAMED(
                new String[] {"ts", "entity", "duration", "won"},
                Types.LOCAL_DATE_TIME, Types.STRING, Types.INT, Types.BOOLEAN));
        
        tableEnv.createTemporaryView(
          "GamePlay",
          victories,
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("entity", DataTypes.STRING())
              .column("duration", DataTypes.INT())
              .column("won", DataTypes.BOOLEAN())
              .watermark("ts", "ts - INTERVAL '1' SECOND")
              .build());

        // Build Purchase table
        DataStream<Row> purchases = env.fromElements(
          Row.of(LocalDateTime.parse("2021-08-21T13:01:02"), "Alice"),
          Row.of(LocalDateTime.parse("2021-08-21T13:01:35"), "Alice"),
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

        // Define features
        tableEnv.createTemporaryView(
          "Features", 
          tableEnv.sqlQuery(
            "SELECT ts, entity, sum(duration) OVER (PARTITION BY entity ORDER BY ts) as loss_duration " +
            "FROM ( SELECT * FROM GamePlay WHERE won = false )"
          ));

        tableEnv.createTemporaryView(
          "FeaturesVer", 
          tableEnv.sqlQuery(
            "SELECT * " +
            "FROM ( " +
            "  SELECT *, " +
            "   ROW_NUMBER() OVER (PARTITION BY entity ORDER BY ts DESC) AS rownum " +
            "  FROM Features) " +
            "WHERE rownum = 1"
          ));
               
        // STEP 2: Define prediction times
        tableEnv.createTemporaryView(
          "Example", 
          tableEnv.sqlQuery(
            "SELECT example.ts, example.entity, features.loss_duration " +
            "FROM ( " +
            "  SELECT " +
            "    ts, " +
            "    entity, " +
            "    count(NULLIF(won,true)) OVER ( " +
            "      PARTITION BY entity " +
            "      ORDER BY ts " +
            "      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW " +
            "    ) as defeat_count " +
            "  FROM GamePlay " +
            ") AS example " +
            "LEFT JOIN FeaturesVer FOR SYSTEM_TIME AS OF example.ts AS features " +
            "ON example.entity = features.entity " +
            "WHERE defeat_count = 2 "
          ));

        // Target value
        tableEnv.createTemporaryView(
          "Target", 
          tableEnv.sqlQuery(
            "SELECT ts, entity, count(1) OVER (PARTITION BY entity ORDER BY ts) as cnt " +
            "FROM Purchase"
          ));

        tableEnv.createTemporaryView(
          "TargetVer", 
          tableEnv.sqlQuery(
            "SELECT * " +
            "FROM ( " +
            "  SELECT *, " +
            "   ROW_NUMBER() OVER (PARTITION BY entity ORDER BY ts DESC) AS rownum " +
            "  FROM Target) " +
            "WHERE rownum = 1"
          ));

        // Combine
        tableEnv.createTemporaryView(
          "ExampleWithTarget", 
          tableEnv.sqlQuery(
            "SELECT Example.ts, Example.entity, Example.loss_duration, target.cnt " +
            "FROM Example " +
            "LEFT JOIN TargetVer FOR SYSTEM_TIME AS OF TIMESTAMPADD(HOUR, 1, Example.ts) AS target " +
            "ON Example.entity = target.entity "
          )); 

        Table resultTable = tableEnv.sqlQuery("SELECT * FROM ExampleWithTarget");

        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print();
        resultStream.writeAsText("output.txt", WriteMode.OVERWRITE);

        env.execute();
    }
}
