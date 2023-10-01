package org.embulk.parser.seqfile.column;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonDouble;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;

public abstract class AbstractFloatWritableColumn extends WritableColumn {

    @FunctionalInterface
    private interface WriteConsumer {
        public void write(PageBuilder pageBuilder, boolean isNull, float value);
    }

    private final WriteConsumer writer;

    public AbstractFloatWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
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

    protected void writeTo(PageBuilder pageBuilder, boolean isNull, float value) {
        writer.write(pageBuilder, isNull, value);
    }

    protected void writeBoolean(PageBuilder pageBuilder, boolean isNull, float value) {
        if (isNull) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setBoolean(column, value != 0);
    }

    protected void writeLong(PageBuilder pageBuilder, boolean isNull, float value) {
        if (isNull) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setLong(column, (long) value);
    }

    protected void writeDouble(PageBuilder pageBuilder, boolean isNull, float value) {
        if (isNull) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setDouble(column, value);
    }

    protected void writeString(PageBuilder pageBuilder, boolean isNull, float value) {
        if (isNull) {
            pageBuilder.setNull(column);
            return;
        }
        String v = Float.toString(value);
        pageBuilder.setString(column, v);
    }

    protected void writeTimestamp(PageBuilder pageBuilder, boolean isNull, float value) {
        if (isNull) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("float->timestamp unsupported");
    }

    protected void writeJson(PageBuilder pageBuilder, boolean isNull, float value) {
        if (isNull) {
            pageBuilder.setNull(column);
            return;
        }
        JsonDouble v = JsonDouble.of(value);
        pageBuilder.setJson(column, v);
    }
}
