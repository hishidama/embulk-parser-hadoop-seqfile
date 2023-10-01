package org.embulk.parser.seqfile.processor;

import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.embulk.config.ConfigException;
import org.embulk.parser.seqfile.column.WritableColumn;
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
import org.embulk.parser.seqfile.writable.EmbulkWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableProcessorFactory {
    private final Logger log = LoggerFactory.getLogger(getClass());

    public WritableProcessor createProcessor(String kv, Writable writable, List<WritableColumn> columnList) {
        if (writable instanceof NullWritable) {
            if (columnList.isEmpty()) {
                return new NullWritableProcessor();
            }
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new NullWritableColumn(from, (NullWritable) writable));
        }
        if (writable instanceof EmbulkWritable) {
            return new EmbulkWritableProcessor((EmbulkWritable) writable, columnList);
        }
        if (writable instanceof Text) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new TextWritableColumn(from, (Text) writable));
        }
        if (writable instanceof VIntWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new VIntWritableColumn(from, (VIntWritable) writable));
        }
        if (writable instanceof VLongWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new VLongWritableColumn(from, (VLongWritable) writable));
        }
        if (writable instanceof BooleanWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new BooleanWritableColumn(from, (BooleanWritable) writable));
        }
        if (writable instanceof ByteWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new ByteWritableColumn(from, (ByteWritable) writable));
        }
        if (writable instanceof ShortWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new ShortWritableColumn(from, (ShortWritable) writable));
        }
        if (writable instanceof IntWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new IntWritableColumn(from, (IntWritable) writable));
        }
        if (writable instanceof LongWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new LongWritableColumn(from, (LongWritable) writable));
        }
        if (writable instanceof FloatWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new FloatWritableColumn(from, (FloatWritable) writable));
        }
        if (writable instanceof DoubleWritable) {
            WritableColumn from = getSingleColumn(kv, columnList);
            return new SimpleWritableProcessor(new DoubleWritableColumn(from, (DoubleWritable) writable));
        }
        throw new UnsupportedOperationException("unsupported WritableProcessor. Writable class: " + writable.getClass().getName());
    }

    protected WritableColumn getSingleColumn(String kv, List<WritableColumn> columnList) {
        switch (columnList.size()) {
        case 0:
            throw new ConfigException(MessageFormat.format("not found {0} columns", kv));
        case 1:
            return columnList.get(0);
        default:
            log.warn("ignore {} columns {}", kv, columnList.subList(1, columnList.size()).stream().map(wc -> wc.getColumn()).collect(Collectors.toList()));
            return columnList.get(0);
        }
    }
}
