package org.embulk.parser.seqfile.column.asakusafw;

import java.io.DataInput;
import java.io.IOException;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractByteWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import com.asakusafw.runtime.value.ByteOption;

public class ByteOptionWritableColumn extends AbstractByteWritableColumn {

    private final ByteOption writableValue = new ByteOption();

    public ByteOptionWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        if (writableValue.isNull()) {
            writeTo(pageBuilder, null);
        } else {
            byte value = writableValue.get();
            writeTo(pageBuilder, value);
        }
    }
}
