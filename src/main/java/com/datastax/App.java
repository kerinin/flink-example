package com.datastax;

import java.time.Duration;
import java.time.LocalDateTime;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

/*
 * 
 * Notes
 * 
 * - The main method caused an error: Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF' left table's time attribute field 
 *   IE - we can't join on arbitrary fields, for example a value shifted forward in time
 * - Re-assigning watermark seems to produce null join results, probably because it's doing a premature read of one side or the other
 * 
 * 
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
          Row.of(LocalDateTime.parse("2021-08-21T02:30:00"), "Alice", 10, true),
          Row.of(LocalDateTime.parse("2021-08-21T02:35:00"), "Bob", 3, false),
          Row.of(LocalDateTime.parse("2021-08-21T03:46:00"), "Bob", 8, false),
          Row.of(LocalDateTime.parse("2021-08-21T03:58:00"), "Bob", 23, true),
          Row.of(LocalDateTime.parse("2021-08-21T04:25:00"), "Bob", 8, true),
          Row.of(LocalDateTime.parse("2021-08-21T05:05:00"), "Alice", 53, true),
          Row.of(LocalDateTime.parse("2021-08-21T05:36:00"), "Alice", 2, false),
          Row.of(LocalDateTime.parse("2021-08-21T07:22:00"), "Bob", 7, false),
          Row.of(LocalDateTime.parse("2021-08-21T08:35:00"), "Alice", 5, false),
          Row.of(LocalDateTime.parse("2021-08-21T10:01:00"), "Alice", 43, true))
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
          Row.of(LocalDateTime.parse("2021-08-21T01:02:00"), "Alice"),
          Row.of(LocalDateTime.parse("2021-08-21T01:35:00"), "Alice"),
          Row.of(LocalDateTime.parse("2021-08-21T03:51:00"), "Bob"))
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
              .watermark("ts", "ts - INTERVAL '2' HOUR")
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
        Table exampleTable = tableEnv.sqlQuery(
          "SELECT example.ts, example.entity, features.loss_duration, TIMESTAMPADD(HOUR, 1, example.ts) AS label_time " +
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
        );
        
        // STEP 3: Shift forward to label time
        tableEnv.createTemporaryView(
          "Example",
          tableEnv.toDataStream(exampleTable),
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("entity", DataTypes.STRING())
              .column("loss_duration", DataTypes.INT())
              .column("label_time", DataTypes.TIMESTAMP(3))
              .watermark("label_time", "label_time - INTERVAL '2' HOUR")
              .build());

        // STEP 4: Append target value
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
        tableEnv.createTemporaryView(
          "ExampleWithTarget", 
          tableEnv.sqlQuery(
            "SELECT Example.ts, Example.label_time, target.ts, Example.entity, Example.loss_duration, target.cnt " +
            "FROM Example " +
            "LEFT JOIN TargetVer FOR SYSTEM_TIME AS OF Example.label_time AS target " +
            "ON Example.entity = target.entity "
          )); 

        Table resultTable = tableEnv.sqlQuery("SELECT * FROM ExampleWithTarget");
        // +I[2021-08-21T03:46, 2021-08-21T04:46, 2021-08-21T03:51, Bob, 11, 1]
        // +I[2021-08-21T08:35, 2021-08-21T09:35, 2021-08-21T01:35, Alice, 7, 2]

        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print();
        resultStream.writeAsText("output.txt", WriteMode.OVERWRITE);

        env.execute();
    }
}
