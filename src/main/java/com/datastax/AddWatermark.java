package com.datastax;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

// NOTE: This might be better as a keyed process function
public class AddWatermark extends ProcessFunction<Row, Row> {
    @Override
    public void processElement(Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        if (value.getKind() == RowKind.INSERT || value.getKind() == RowKind.UPDATE_AFTER) {
            // NOTE: Pulling this from the watermark is a potential source of temporal
            // leakage.
            // It would be much preferable to access the event time, but it doesn't appear
            // to be preserved from the input table to the changelog
            LocalDateTime rowTime = Instant.ofEpochMilli(ctx.timerService().currentWatermark()).atZone(ZoneOffset.UTC)
                    .toLocalDateTime();
            out.collect(Row.join(Row.ofKind(RowKind.INSERT, rowTime), value));
        }
    }
}
