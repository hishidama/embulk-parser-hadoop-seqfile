package org.embulk.parser.seqfile.column;

import java.time.Instant;
import java.time.LocalDateTime;

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

public abstract class AbstractLocalDateTimeWritableColumn extends WritableColumn {

    @FunctionalInterface
    private interface WriteConsumer {
        public void write(PageBuilder pageBuilder, LocalDateTime value);
    }

    private final WriteConsumer writer;

    public AbstractLocalDateTimeWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
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

    protected void writeTo(PageBuilder pageBuilder, LocalDateTime value) {
        writer.write(pageBuilder, value);
    }

    protected void writeBoolean(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("datetime->boolean unsupported");
    }

    protected void writeLong(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("datetime->long unsupported");
    }

    protected void writeDouble(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("datetime->double unsupported");
    }

    protected void writeString(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        String v = value.toString();
        pageBuilder.setString(column, v);
    }

    protected void writeTimestamp(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        Instant v = value.toInstant(getZoneOffset());
        pageBuilder.setTimestamp(column, v);
    }

    protected void writeJson(PageBuilder pageBuilder, LocalDateTime value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        JsonString v = JsonString.of(value.toString());
        pageBuilder.setJson(column, v);
    }
}
