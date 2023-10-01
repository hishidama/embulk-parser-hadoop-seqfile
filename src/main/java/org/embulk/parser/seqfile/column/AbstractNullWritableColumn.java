package org.embulk.parser.seqfile.column;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;

public abstract class AbstractNullWritableColumn extends WritableColumn {

    @FunctionalInterface
    private interface WriteConsumer {
        public void write(PageBuilder pageBuilder);
    }

    private final WriteConsumer writer;

    public AbstractNullWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        super(task, column, option);
        this.writer = getWriter(column);
    }

    protected WriteConsumer getWriter(Column column) {
        Type type = column.getType();
        if (type instanceof BooleanType) {
            return this::writeBoolean;
        } else if (type instanceof LongType) {
            return this::writeLong;
        } else if (type instanceof DoubleType) {
            return this::writeDouble;
        } else if (type instanceof StringType) {
            return this::writeString;
        } else if (type instanceof TimestampType) {
            return this::writeTimestamp;
        } else if (type instanceof JsonType) {
            return this::writeJson;
        } else {
            throw new IllegalArgumentException("Column has an unexpected type: " + type);
        }
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        writer.write(pageBuilder);
    }

    protected void writeBoolean(PageBuilder pageBuilder) {
        pageBuilder.setNull(column);
    }

    protected void writeLong(PageBuilder pageBuilder) {
        pageBuilder.setNull(column);
    }

    protected void writeDouble(PageBuilder pageBuilder) {
        pageBuilder.setNull(column);
    }

    protected void writeString(PageBuilder pageBuilder) {
        pageBuilder.setNull(column);
    }

    protected void writeTimestamp(PageBuilder pageBuilder) {
        pageBuilder.setNull(column);
    }

    protected void writeJson(PageBuilder pageBuilder) {
        pageBuilder.setNull(column);
    }
}
