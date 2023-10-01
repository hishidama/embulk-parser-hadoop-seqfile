package org.embulk.parser.seqfile.column.asakusafw;

import java.io.DataInput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractLocalDateTimeWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;

public class DateTimeOptionWritableColumn extends AbstractLocalDateTimeWritableColumn {

    private final DateTimeOption writableValue = new DateTimeOption();

    public DateTimeOptionWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        DateTime value = writableValue.or(null);
        if (value == null) {
            writeTo(pageBuilder, null);
        } else {
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(value.getElapsedSeconds() - 62135596800L, 0, ZoneOffset.UTC);
            writeTo(pageBuilder, dateTime);
        }
    }

    @Override
    protected void writeLong(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            super.writeLong(pageBuilder, value);
            return;
        }
        pageBuilder.setLong(column, writableValue.get().getElapsedSeconds());
    }

    @Override
    protected void writeDouble(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            super.writeDouble(pageBuilder, value);
            return;
        }
        pageBuilder.setDouble(column, writableValue.get().getElapsedSeconds());
    }
}
