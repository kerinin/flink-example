package com.datastax;

import org.apache.flink.table.api.*;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;


public class App 
{
    public static void main(String[] args)
        {
          EnvironmentSettings settings = EnvironmentSettings
              .newInstance()
              .inStreamingMode()
              //.inBatchMode()
              .build();
    
          // Create a TableEnvironment for batch or streaming execution.
          // See the "Create a TableEnvironment" section for details.
          TableEnvironment tableEnv = TableEnvironment.create(settings);
    
          // Create a source table
          tableEnv.createTemporaryTable("RawSourceTable", TableDescriptor.forConnector("datagen")
            .schema(Schema.newBuilder()
              .column("ts", DataTypes.TIMESTAMP_LTZ())
              .column("entity", DataTypes.STRING())
              .column("val", DataTypes.INT())
              .build())
            .option(DataGenConnectorOptions.ROWS_PER_SECOND, 1L)
            .option(DataGenConnectorOptions.NUMBER_OF_ROWS, 10L)
            .build());

          // Cleanup generated data
          // This just truncates the entity ID so we can group by it.
          tableEnv.createTemporaryView("SourceTable", tableEnv.sqlQuery(
            "SELECT *, LEFT(entity, 1) as entity_short " +
            "FROM RawSourceTable "
          ));

          // Create a Table object from a SQL query
          Table table2 = tableEnv.sqlQuery(
            "SELECT entity_short, count(*) " +
            "FROM SourceTable " +
            "GROUP BY entity_short"
          );

          // Execute and dump to STDOUT
          table2.execute().print();
    }
}
