package org.embulk.parser.seqfile.column.asakusafw;

import java.io.DataInput;
import java.io.IOException;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractShortWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import com.asakusafw.runtime.value.ShortOption;

public class ShortOptionWritableColumn extends AbstractShortWritableColumn {

    private final ShortOption writableValue = new ShortOption();

    public ShortOptionWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        if (writableValue.isNull()) {
            writeTo(pageBuilder, true, (short) 0);
        } else {
            short value = writableValue.get();
            writeTo(pageBuilder, false, value);
        }
    }
}
