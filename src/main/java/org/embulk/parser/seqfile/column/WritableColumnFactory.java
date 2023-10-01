package org.embulk.parser.seqfile.column;

import org.embulk.config.ConfigException;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.ColumnOptionTask;
import org.embulk.parser.seqfile.SequenceFileParserPlugin.PluginTask;
import org.embulk.parser.seqfile.column.asakusafw.BooleanOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.ByteOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.DateOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.DateTimeOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.DecimalOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.DoubleOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.FloatOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.IntOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.LongOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.ShortOptionWritableColumn;
import org.embulk.parser.seqfile.column.asakusafw.StringOptionWritableColumn;
import org.embulk.parser.seqfile.column.simple.BooleanWritableColumn;
import org.embulk.parser.seqfile.column.simple.ByteWritableColumn;
import org.embulk.parser.seqfile.column.simple.DoubleWritableColumn;
import org.embulk.parser.seqfile.column.simple.FloatWritableColumn;
import org.embulk.parser.seqfile.column.simple.IntWritableColumn;
import org.embulk.parser.seqfile.column.simple.LongWritableColumn;
import org.embulk.parser.seqfile.column.simple.NullWritableColumn;
import org.embulk.parser.seqfile.column.simple.ShortWritableColumn;
import org.embulk.parser.seqfile.column.simple.TextWritableColumn;
import org.embulk.parser.seqfile.column.simple.VIntWritableColumn;
import org.embulk.parser.seqfile.column.simple.VLongWritableColumn;
import org.embulk.spi.Column;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;

public class WritableColumnFactory {

    public WritableColumn createColumn(PluginTask task, Column column, ColumnOptionTask option) {
        WritableType writableType = getWritableType(column, option);
        switch (writableType) {
        case NULL:
            return new NullWritableColumn(task, column, option);
        case BOOLEAN:
            return new BooleanWritableColumn(task, column, option);
        case BYTE:
            return new ByteWritableColumn(task, column, option);
        case SHORT:
            return new ShortWritableColumn(task, column, option);
        case INT:
            return new IntWritableColumn(task, column, option);
        case LONG:
            return new LongWritableColumn(task, column, option);
        case FLOAT:
            return new FloatWritableColumn(task, column, option);
        case DOUBLE:
            return new DoubleWritableColumn(task, column, option);
        case VINT:
            return new VIntWritableColumn(task, column, option);
        case VLONG:
            return new VLongWritableColumn(task, column, option);
        case TEXT:
            return new TextWritableColumn(task, column, option);
        case BOOLEAN_OPTION:
            return new BooleanOptionWritableColumn(task, column, option);
        case BYTE_OPTION:
            return new ByteOptionWritableColumn(task, column, option);
        case SHORT_OPTION:
            return new ShortOptionWritableColumn(task, column, option);
        case INT_OPTION:
            return new IntOptionWritableColumn(task, column, option);
        case LONG_OPTION:
            return new LongOptionWritableColumn(task, column, option);
        case FLOAT_OPTION:
            return new FloatOptionWritableColumn(task, column, option);
        case DOUBLE_OPTION:
            return new DoubleOptionWritableColumn(task, column, option);
        case DECIMAL_OPTION:
            return new DecimalOptionWritableColumn(task, column, option);
        case STRING_OPTION:
            return new StringOptionWritableColumn(task, column, option);
        case DATE_OPTION:
            return new DateOptionWritableColumn(task, column, option);
        case DATETIME_OPTION:
            return new DateTimeOptionWritableColumn(task, column, option);
        default:
            throw new AssertionError("WritableColumnFactory unsupported yet. wtype: " + writableType);
        }
    }

    protected WritableType getWritableType(Column column, ColumnOptionTask option) {
        WritableType writableType = option.getWritableType().orElse(null);
        if (writableType != null) {
            return writableType;
        }

        Type type = column.getType();
        if (type instanceof BooleanType) {
            return WritableType.BOOLEAN;
        } else if (type instanceof LongType) {
            return WritableType.LONG;
        } else if (type instanceof DoubleType) {
            return WritableType.DOUBLE;
        } else if (type instanceof StringType) {
            return WritableType.TEXT;
        } else if (type instanceof TimestampType) {
            throw new ConfigException("wtype not specified. " + column);
        } else if (type instanceof JsonType) {
            throw new ConfigException("wtype not specified. " + column);
        } else {
            throw new IllegalArgumentException("Column has an unexpected type: " + type);
        }
    }
}
