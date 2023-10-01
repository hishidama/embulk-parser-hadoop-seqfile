package org.embulk.parser.seqfile.column.simple;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractNullWritableColumn;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.Column;

public class NullWritableColumn extends AbstractNullWritableColumn {

    private final NullWritable writableValue;

    public NullWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        this.writableValue = NullWritable.get();
    }

    public NullWritableColumn(WritableColumn from, NullWritable writableValue) {
        super(from.getPluginTask(), from.getColumn(), from.getColumnOption());
        this.writableValue = writableValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }
}
