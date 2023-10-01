package org.embulk.parser.seqfile.column.simple;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractFloatWritableColumn;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class FloatWritableColumn extends AbstractFloatWritableColumn {

    private final FloatWritable writableValue;

    public FloatWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        writableValue = new FloatWritable();
    }

    public FloatWritableColumn(WritableColumn from, FloatWritable writableValue) {
        super(from.getPluginTask(), from.getColumn(), from.getColumnOption());
        this.writableValue = writableValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        float value = writableValue.get();
        writeTo(pageBuilder, false, value);
    }
}
