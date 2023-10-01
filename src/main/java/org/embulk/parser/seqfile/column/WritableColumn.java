package org.embulk.parser.seqfile.column;

import java.io.DataInput;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

public abstract class WritableColumn {

    protected final PluginTask task;
    protected final Column column;
    protected final ColumnOptionTask option;

    public WritableColumn(PluginTask task, Column column, ColumnOptionTask option) {
        this.task = task;
        this.column = column;
        this.option = option;
    }

    public PluginTask getPluginTask() {
        return this.task;
    }

    public Column getColumn() {
        return this.column;
    }

    public ColumnOptionTask getColumnOption() {
        return this.option;
    }

    public abstract void readFields(DataInput in) throws IOException;

    public abstract void writeTo(PageBuilder pageBuilder);

    private TimestampFormatter timestampFormatter = null;

    protected TimestampFormatter getTimestampFormatter() {
        if (this.timestampFormatter == null) {
            String format = option.getFormat().orElse(task.getDefaultTimestampFormat());
            String zone = getZoneIdString();
            String date = option.getDate().orElse(task.getDefaultDate());
            this.timestampFormatter = TimestampFormatter.builder(format, true) //
                    .setDefaultZoneFromString(zone) //
                    .setDefaultDateFromString(date) //
                    .build();
        }
        return this.timestampFormatter;
    }

    private ZoneId zoneId = null;

    protected ZoneId getZoneId() {
        if (this.zoneId == null) {
            String zone = getZoneIdString();
            String id = ZoneId.SHORT_IDS.get(zone);
            if (id != null) {
                zone = id;
            }
            this.zoneId = ZoneId.of(zone);
        }
        return zoneId;
    }

    private ZoneOffset zoneOffset = null;

    protected ZoneOffset getZoneOffset() {
        if (this.zoneOffset == null) {
            this.zoneOffset = getZoneId().getRules().getOffset(Instant.now());
        }
        return zoneOffset;
    }

    protected String getZoneIdString() {
        return option.getTimeZoneId().orElse(task.getDefaultTimeZoneId());
    }
}
