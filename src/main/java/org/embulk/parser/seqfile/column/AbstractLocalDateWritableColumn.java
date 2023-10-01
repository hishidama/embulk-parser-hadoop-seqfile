package org.embulk.parser.seqfile.column;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

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

public abstract class AbstractLocalDateWritableColumn extends WritableColumn {

    @FunctionalInterface
    private interface WriteConsumer {
        public void write(PageBuilder pageBuilder, LocalDate value);
    }

    private final WriteConsumer writer;

    public AbstractLocalDateWritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
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

    protected void writeTo(PageBuilder pageBuilder, LocalDate value) {
        writer.write(pageBuilder, value);
    }

    protected void writeBoolean(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("date->boolean unsupported");
    }

    protected void writeLong(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("date->long unsupported");
    }

    protected void writeDouble(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        throw new UnsupportedOperationException("date->double unsupported");
    }

    protected void writeString(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        String v = value.toString();
        pageBuilder.setString(column, v);
    }

    protected void writeTimestamp(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        ZonedDateTime zdate = ZonedDateTime.of(value, LocalTime.MIN, getZoneId());
        Instant v = zdate.toInstant();
        pageBuilder.setTimestamp(column, v);
    }

    protected void writeJson(PageBuilder pageBuilder, LocalDate value) {
        if (value == null) {
            pageBuilder.setNull(column);
            return;
        }
        JsonString v = JsonString.of(value.toString());
        pageBuilder.setJson(column, v);
    }
}
