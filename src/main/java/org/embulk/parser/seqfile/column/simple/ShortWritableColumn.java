package org.embulk.parser.seqfile.column.simple;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.ShortWritable;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractShortWritableColumn;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class ShortWritableColumn extends AbstractShortWritableColumn {

    private final ShortWritable writableValue;

    public ShortWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        writableValue = new ShortWritable();
    }

    public ShortWritableColumn(WritableColumn from, ShortWritable writableValue) {
        super(from.getPluginTask(), from.getColumn(), from.getColumnOption());
        this.writableValue = writableValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        short value = writableValue.get();
        writeTo(pageBuilder, false, value);
    }
}
