package org.embulk.parser.seqfile.column;

import java.time.Instant;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonString;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.util.timestamp.TimestampFormatter;

public abstract class AbstractStringWritableColumn extends WritableColumn {

    @FunctionalInterface
    private interface WriteConsumer {
        public void write(PageBuilder pageBuilder, String value);
    }

    private final WriteConsumer writer;

    public AbstractStringWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
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

    protected void writeTo(PageBuilder pageBuilder, String value) {
        writer.write(pageBuilder, value);
    }

    protected void writeBoolean(PageBuilder pageBuilder, String value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        boolean v = Boolean.parseBoolean(value);
        pageBuilder.setBoolean(column, v);
    }

    protected void writeLong(PageBuilder pageBuilder, String value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        long v = Long.parseLong(value);
        pageBuilder.setLong(column, v);
    }

    protected void writeDouble(PageBuilder pageBuilder, String value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        double v = Double.parseDouble(value);
        pageBuilder.setDouble(column, v);
    }

    protected void writeString(PageBuilder pageBuilder, String value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setString(column, value);
    }

    protected void writeTimestamp(PageBuilder pageBuilder, String value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        TimestampFormatter formatter = getTimestampFormatter();
        Instant v = formatter.parse(value);
        pageBuilder.setTimestamp(column, v);
    }

    protected void writeJson(PageBuilder pageBuilder, String value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        JsonString v = JsonString.of(value);
        pageBuilder.setJson(column, v);
    }
}
