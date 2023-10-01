package org.embulk.parser.seqfile.column.simple;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractBooleanWritableColumn;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class BooleanWritableColumn extends AbstractBooleanWritableColumn {

    private final BooleanWritable writableValue;

    public BooleanWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        this.writableValue = new BooleanWritable();
    }

    public BooleanWritableColumn(WritableColumn from, BooleanWritable writableValue) {
        super(from.getPluginTask(), from.getColumn(), from.getColumnOption());
        this.writableValue = writableValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        boolean value = writableValue.get();
        writeTo(pageBuilder, value);
    }
}
