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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*
         * Real-time ML example
         * 
         * Questions:
         * 
         * - How is a temporal join different than a vanilla join consumed as a changelog?
         * 
         *    I think the difference has to do with fixing values computed from a stream to an instant.
         *    A vanilla join A<->B produces an evolving relation between the two keys as events are added to tables.
         *    OTOH, a temporal join (A,t)<->B@t produces a relation between the tuple (A,t) and the value of key B at time t. 
         *    The result is that the value B@t doesn't change, even if new events affect the value of B.
         *    Note that both A and t may change over time, but for a _given_ tuple, B@t is constant.
         * 
         */


        // Create GamePlay and Purchase tables and populate them with some rows.
        // 
        // These are "event" tables where each row is associated with a time.
        setupTables(env, tableEnv);

        // Define features. 
        //
        // Note that this is a "vanilla" SQL expression with no concept of time.
        // This query can be used unmodified to populate a feature cache.
        String features = "SELECT user AS _entity, sum(duration) as loss_duration FROM GamePlay WHERE won = false GROUP BY user";

        // Define target
        //
        // Note again that this query has no notion of time.
        String target = "SELECT user AS _entity, count(1) as cnt FROM Purchase GROUP BY user";

        // Create an example each time a player loses for the second time in a row.
        //
        // This query should produce rows describing training examples.
        // Each example specifies the entity for which to compute the example, the prediction time at which the features should be observed and the label time at which the target should be observed.
        // 
        // This is a "vanilla" query that returns time values - it doesn't depend on any unusual "streaming SQL" features.
        //
        // The inner selection does a windowed aggregation over 2 consecutive game play events and counts how many of them were lost.
        // The wrapping selection then filters rows where the loss count is two.
        // The time of the second loss is used as the prediction time, and an hour later is used as the label time.
        String examples = 
          "SELECT user AS _entity, ts AS _prediction_time, TIMESTAMPADD(HOUR, 1, ts) AS _label_time " +
          "FROM ( " +
          "  SELECT " +
          "    user, " +
          "    ts, " +
          "    count(NULLIF(won,true)) OVER ( " +
          "      PARTITION BY user " +
          "      ORDER BY ts " +
          "      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW " +
          "    ) as defeat_count " +
          "  FROM GamePlay " +
          ")" +
          "WHERE defeat_count = 2 ";

        Table exampleTable = createTrainingExamples(tableEnv, features, target, examples);
        // +I[Bob, 2021-08-21T03:46, 2021-08-21T04:46, 11, 1]
        // +I[Alice, 2021-08-21T08:35, 2021-08-21T09:35, 7, 2]

        DataStream<Row> exampleStream = tableEnv.toChangelogStream(exampleTable);
        exampleStream.print();
        exampleStream.writeAsText("output.txt", WriteMode.OVERWRITE);

        env.execute();
    }

    static void setupTables(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // Build GamePlay table
        // 
        // This builds each row and then assigns names and types to the fields.
        //
        // To make the rows available as a SQL table, we register it as a named view.
        // This needs a second schema definition - notice that this is where we're
        // telling Flink what the "event time" of each row is, and defining the
        // allowed lateness for events.
        tableEnv.createTemporaryView(
          "GamePlay",
          env.fromElements(
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
                  new String[] {"ts", "user", "duration", "won"},
                  Types.LOCAL_DATE_TIME, Types.STRING, Types.INT, Types.BOOLEAN)),
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3).notNull())
              .column("user", DataTypes.STRING().notNull())
              .column("duration", DataTypes.INT())
              .column("won", DataTypes.BOOLEAN())
              .watermark("ts", "ts")
              .build());

        // Build Purchase table
        // Same as for GamePlay
        tableEnv.createTemporaryView(
          "Purchase",
          env.fromElements(
            Row.of(LocalDateTime.parse("2021-08-21T01:02:00"), "Alice"),
            Row.of(LocalDateTime.parse("2021-08-21T01:35:00"), "Alice"),
            Row.of(LocalDateTime.parse("2021-08-21T03:51:00"), "Bob"))
            .returns(
              Types.ROW_NAMED(
                  new String[] {"ts", "user"},
                  Types.LOCAL_DATE_TIME, Types.STRING)),
          Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP(3))
              .column("user", DataTypes.STRING())
              .watermark("ts", "ts")
              .build());
    }

    static Table createTrainingExamples(StreamTableEnvironment tableEnv, String features, String target, String examples) throws Exception {
        // Define features
        // 
        // These features are going to be consumed to build a versioned table.
        // This requires that each row have a key and a timestamp.
        // The versioned row exposes the most recent row for each key, and can be used as the RHS of a temporal join.
        //
        // NOTE: This use of "AddWatermark" is the jankiest part of this prototype.
        // In order to build a versioned table, we need a time value to associate with each row in the changelog produced by the original query.
        // Unfortunately, Flink doesn't seem to preserve the event time of the SQL query once it's converted to a changelog stream.
        // The hack used here is to access the _watermark_ of each changelog row rather than it's timestamp.
        //
        // The watermark is _related_ to event time but not strictly the same - in most cases the watermark is delayed by a fixed amount from event time to accommodate late data.
        // A better solution would be to assign changelog rows the event time at which they were computed.
        // Another possibilty (that doens't require changes to Flink) would be to parameterize watermark offset, allowing us to deterministically reconstruct event time.
        tableEnv.createTemporaryView(
          "Features",
          tableEnv.toChangelogStream(
            tableEnv.sqlQuery(features))
              .process(new AddWatermark())
              .returns(Types.ROW_NAMED(
                  new String[] {"_change_time", "_entity", "loss_duration"}, // NOTE: This hard-codes feature (and later, target) column names and types, but it should be possible to use schema reflection to do this generically.
                  Types.LOCAL_DATE_TIME, Types.STRING, Types.INT)),
          Schema.newBuilder()
              .column("_change_time", DataTypes.TIMESTAMP(3).notNull())
              .column("_entity", DataTypes.STRING().notNull())
              .column("loss_duration", DataTypes.INT())
              .primaryKey("_entity")
              .watermark("_change_time", "_change_time")
              .build()
        );
               
        // Define target
        //
        // This follows the same pattern as the features, as we'll be joining these results in the same way.
        tableEnv.createTemporaryView(
          "Target",
          tableEnv.toChangelogStream(
            tableEnv.sqlQuery(target))
              .process(new AddWatermark())
              .returns(Types.ROW_NAMED(
                  new String[] {"_change_time", "_entity", "cnt"},
                  Types.LOCAL_DATE_TIME, Types.STRING, Types.LONG)),
          Schema.newBuilder()
              .column("_change_time", DataTypes.TIMESTAMP(3).notNull())
              .column("_entity", DataTypes.STRING().notNull())
              .column("cnt", DataTypes.BIGINT())
              .primaryKey("_entity")
              .watermark("_change_time", "_change_time")
              .build()
        );

        // Join features into examples.
        //
        // This is a "temporal join", where each LHS row includes a timestamp, and each RHS row produces the value of a key "as-of" that time.
        // The LHS timestamp must be the event time of each row, and the RHS must be a "versioned table".
        // This query is the reason we did all that stuff above to convert a standard query into a versioned table.
        Table exampleTable = tableEnv.sqlQuery(
          "WITH example AS ( " + examples + ")" +
          "SELECT example._entity, example._prediction_time, example._label_time, features.loss_duration " +
          "FROM example " +
          "LEFT JOIN Features FOR SYSTEM_TIME AS OF example._prediction_time AS features " +
          "ON example._entity = features._entity ");
        
        // Shift examples forward to label time
        //
        // Temporal joins are only supported with respect to the event time of the join's LHS.
        // Naively, this would prevent us from joining tables as-of different points in time (ie prediciton time as well as label time).
        // This plays some tricks with Flink's API's to re-assign the event time associated with each event to work around this limitation. 
        //
        // First, the examples are converted to the "DataStream" API, then a temporary view (in the Table API) is built from the datastream.
        // When we build the view we can specify a new schema, and part of the schema is the watermark column.
        // This allows us to _change_ which column is used as the watermark from `_prediction_time` to `_label_time`.
        tableEnv.createTemporaryView(
          "example",
          tableEnv.toDataStream(exampleTable),
          Schema.newBuilder()
              .column("_entity", DataTypes.STRING())
              .column("_prediction_time", DataTypes.TIMESTAMP(3))
              .column("_label_time", DataTypes.TIMESTAMP(3))
              .column("loss_duration", DataTypes.INT())
              .watermark("_label_time", "_label_time")
              .build());

        // Finally, we join targets into examples using a temporal join relative to label time.
        return tableEnv.sqlQuery(
            "SELECT example._entity, example._prediction_time, example._label_time, example.loss_duration, target.cnt " +
            "FROM example " +
            "LEFT JOIN Target FOR SYSTEM_TIME AS OF example._label_time AS target " +
            "ON example._entity = target._entity "
        );
    }
}
