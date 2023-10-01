package org.embulk.parser.seqfile.column.asakusafw;

import java.io.DataInput;
import java.io.IOException;
import java.time.LocalDate;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.AbstractLocalDateWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;

public class DateOptionWritableColumn extends AbstractLocalDateWritableColumn {

    private final DateOption writableValue = new DateOption();

    public DateOptionWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        writableValue.readFields(in);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        Date value = writableValue.or(null);
        if (value == null) {
            writeTo(pageBuilder, null);
        } else {
            LocalDate date = LocalDate.ofEpochDay(value.getElapsedDays() - 719162);
            writeTo(pageBuilder, date);
        }
    }

    @Override
    protected void writeLong(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            super.writeLong(pageBuilder, value);
            return;
        }
        pageBuilder.setLong(column, writableValue.get().getElapsedDays());
    }

    @Override
    protected void writeDouble(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            super.writeDouble(pageBuilder, value);
            return;
        }
        pageBuilder.setDouble(column, writableValue.get().getElapsedDays());
    }
}
