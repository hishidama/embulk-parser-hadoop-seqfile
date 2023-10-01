package org.embulk.parser.seqfile.column.asakusafw;

import java.io.DataInput;
import java.io.IOException;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractDoubleWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import com.asakusafw.runtime.value.DoubleOption;

public class DoubleOptionWritableColumn extends AbstractDoubleWritableColumn {

    private final DoubleOption writableValue = new DoubleOption();

    public DoubleOptionWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        if (writableValue.isNull()) {
            writeTo(pageBuilder, true, 0);
        } else {
            double value = writableValue.get();
            writeTo(pageBuilder, false, value);
        }
    }
}
