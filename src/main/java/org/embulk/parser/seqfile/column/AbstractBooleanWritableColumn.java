package org.embulk.parser.seqfile.column;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonBoolean;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;

public abstract class AbstractBooleanWritableColumn extends WritableColumn {

    @FunctionalInterface
    private interface WriteConsumer {
        public void write(PageBuilder pageBuilder, Boolean value);
    }

    private final WriteConsumer writer;

    public AbstractBooleanWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
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

    protected void writeTo(PageBuilder pageBuilder, Boolean value) {
        writer.write(pageBuilder, value);
    }

    protected void writeBoolean(PageBuilder pageBuilder, Boolean value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setBoolean(column, value);
    }

    protected void writeLong(PageBuilder pageBuilder, Boolean value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setLong(column, value ? 1 : 0);
    }

    protected void writeDouble(PageBuilder pageBuilder, Boolean value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setDouble(column, value ? 1 : 0);
    }

    protected void writeString(PageBuilder pageBuilder, Boolean value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        String v = Boolean.toString(value);
        pageBuilder.setString(column, v);
    }

    protected void writeTimestamp(PageBuilder pageBuilder, Boolean value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("boolean->timestamp unsupported");
    }

    protected void writeJson(PageBuilder pageBuilder, Boolean value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        JsonBoolean v = JsonBoolean.of(value);
        pageBuilder.setJson(column, v);
    }
}
