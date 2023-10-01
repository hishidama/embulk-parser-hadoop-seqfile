package org.embulk.parser.seqfile.column.asakusafw;

import java.io.DataInput;
import java.io.IOException;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractStringWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import com.asakusafw.runtime.value.StringOption;

public class StringOptionWritableColumn extends AbstractStringWritableColumn {

    private final StringOption writableValue = new StringOption();

    public StringOptionWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        String value = writableValue.or((String) null);
        writeTo(pageBuilder, value);
    }
}
