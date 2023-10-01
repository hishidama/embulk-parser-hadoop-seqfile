package org.embulk.parser.seqfile.column.simple;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractLongWritableColumn;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class LongWritableColumn extends AbstractLongWritableColumn {

    private final LongWritable writableValue;

    public LongWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        writableValue = new LongWritable();
    }

    public LongWritableColumn(WritableColumn from, LongWritable writableValue) {
        super(from.getPluginTask(), from.getColumn(), from.getColumnOption());
        this.writableValue = writableValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        long value = writableValue.get();
        writeTo(pageBuilder, false, value);
    }
}
