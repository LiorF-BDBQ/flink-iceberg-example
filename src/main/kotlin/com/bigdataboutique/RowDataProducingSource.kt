package com.bigdataboutique

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.StringData
import org.apache.flink.table.data.TimestampData
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

class RowDataProducingSource : RichParallelSourceFunction<RowData>() {
    @Volatile
    private var running = true

    @Throws(Exception::class)
    override fun run(ctx: SourceFunction.SourceContext<RowData>) {
        while (running) {
            val item = GenericRowData(3)
            item.setField(0, StringData.fromString("hello"))
            item.setField(1, StringData.fromString("world"))
            item.setField(2, TimestampData.fromLocalDateTime(LocalDateTime.now()))
            ctx.collect(item)
            TimeUnit.SECONDS.sleep(2)
        }
    }

    override fun cancel() {
        running = false
    }
}
