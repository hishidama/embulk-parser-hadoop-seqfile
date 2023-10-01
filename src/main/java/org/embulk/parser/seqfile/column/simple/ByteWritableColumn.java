package org.embulk.parser.seqfile.column.simple;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractByteWritableColumn;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class ByteWritableColumn extends AbstractByteWritableColumn {

    private final ByteWritable writableValue;

    public ByteWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        writableValue = new ByteWritable();
    }

    public ByteWritableColumn(WritableColumn from, ByteWritable writableValue) {
        super(from.getPluginTask(), from.getColumn(), from.getColumnOption());
        this.writableValue = writableValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        byte value = writableValue.get();
        writeTo(pageBuilder, value);
    }
}
