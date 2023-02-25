package com.datastax;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App 
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception
        {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Real-time ML example
        // This roughly follows the example in the slides: https://docs.google.com/presentation/d/1DEuk2NDiujmzIHAHDTX_dc8-sUHqf0h1KSl4HdpWKpQ/edit#slide=id.p12



        // Build GamePlay table
        // 
        // This builds each row and then assigns names and types to the fields.
        //
        // To make the rows available as a SQL table, we register it as a named view.
        // This needs a second schema definition - notice that this is where we're
        // telling Flink what the "event time" of each row is, and defining the
        // allowed lateness for events.
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
              .watermark("ts", "ts")
              .build());

        // Build Purchase table
        // Same as for GamePlay
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
              .watermark("ts", "ts")
              .build());

        // STEP 1: Define features
        // These features are going to be consumed to build a versioned table.
        // This requires that each row have a key and a timestamp - the versioned row exposes
        // the most recent row for each key, and can be used as the RHS of a temporal join
        tableEnv.createTemporaryView(
          "Features",
          tableEnv.toChangelogStream(
          tableEnv.sqlQuery( "SELECT entity, sum(duration) as loss_duration FROM GamePlay WHERE won = false GROUP BY entity"))
            .process(new AddWatermark())
            .returns(Types.ROW_NAMED(
                new String[] {"ts", "entity", "loss_duration"},
                Types.LOCAL_DATE_TIME, Types.STRING, Types.INT)),
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3).notNull())
              .column("entity", DataTypes.STRING().notNull())
              .column("loss_duration", DataTypes.INT())
              .primaryKey("entity")
              .watermark("ts", "ts")
              .build()
        );
               
        // STEP 2: Define prediction times
        //
        // The SQL for this is ugly. The inner selection does a windowed aggregation over 2 consecutive game
        // play events and counts how many of them were lost.
        //
        // The wrapping selection then filters rows where the loss count is two.
        //
        // Finally, the feature values are pulled in for each example time using a temporal join
        // against the versioned features table.
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
          "LEFT JOIN Features FOR SYSTEM_TIME AS OF example.ts AS features " +
          "ON example.entity = features.entity " +
          "WHERE defeat_count = 2 "
        );
        
        // STEP 3: Shift forward to label time
        //
        // This plays some tricks with Flink's API's to re-assign the event time associated
        // with each event. First, we calculated the desired label time as one of the features
        // (this was part of the previous step). Next, the example are converted to the "DataStream" 
        // API and then a view (in the Table API) is built from the data stream. This allows us to 
        // provide a new schema for the view, where the label time is used for the watermark. 
        tableEnv.createTemporaryView(
          "Example",
          tableEnv.toDataStream(exampleTable),
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("entity", DataTypes.STRING())
              .column("loss_duration", DataTypes.INT())
              .column("label_time", DataTypes.TIMESTAMP(3))
              .watermark("label_time", "label_time - INTERVAL '1' MINUTE")
              .build());

        // STEP 4: Append target value
        //
        // This is a relatively straightforward temporal join and behaves about the same
        // as when we joined the features in to begin with. This is only really interesting
        // becasue it demonstrates that the re-assigned event time is being used for the
        // point-in-time join against target values.
        tableEnv.createTemporaryView(
          "Target",
          tableEnv.toChangelogStream(
          tableEnv.sqlQuery( "SELECT entity, count(1) as cnt FROM Purchase GROUP BY entity"))
            .process(new AddWatermark())
            .returns(Types.ROW_NAMED(
                new String[] {"ts", "entity", "cnt"},
                Types.LOCAL_DATE_TIME, Types.STRING, Types.LONG)),
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3).notNull())
              .column("entity", DataTypes.STRING().notNull())
              .column("cnt", DataTypes.BIGINT())
              .primaryKey("entity")
              .watermark("ts", "ts")
              .build()
        );


        tableEnv.createTemporaryView(
          "ExampleWithTarget", 
          tableEnv.sqlQuery(
            "SELECT Example.ts, Example.label_time, target.ts, Example.entity, Example.loss_duration, target.cnt " +
            "FROM Example " +
            "LEFT JOIN Target FOR SYSTEM_TIME AS OF Example.label_time AS target " +
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
