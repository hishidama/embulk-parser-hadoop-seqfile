package com.example.hadoop.seqfile;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.ValueOption;

public class SeqFileGenerator {

    private static String outputDir;

    // -Dhadoop.home.dir=$HADDOP_HOME
    public static void main(String[] args) throws IOException {
        outputDir = args[0];

        writeExample();
        writeAsakusa();
        writeWordCount();
    }

    static void writeExample() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("file://" + outputDir + "/example.dat");

        NullWritable key = NullWritable.get();
        ExampleWritable1 val = new ExampleWritable1();

        Option fileOpt = Writer.file(path);
        Option keyOpt = Writer.keyClass(key.getClass());
        Option valOpt = Writer.valueClass(val.getClass());
        Option compOpt = Writer.compression(CompressionType.NONE);
        try (Writer writer = SequenceFile.createWriter(conf, fileOpt, keyOpt, valOpt, compOpt)) {
            fill(val, 100);
            writer.append(key, val);
            fill(val, 201);
            writer.append(key, val);
        }
    }

    private static void fill(ExampleWritable1 model, int value) {
        model.booleanValue.set((value % 2) == 0);
        model.byteValue.set((byte) (value - 2));
        model.shortValue.set((short) (value - 1));
        model.intValue.set(value);
        model.longValue.set(value + 1);
        model.floatValue.set(value + 2);
        model.doubleValue.set(value + 3);
        model.vintValue.set(value + 4);
        model.vlongValue.set(value + 5);
        model.stringValue.set(Integer.toString(value + 6));
    }

    static void writeAsakusa() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("file://" + outputDir + "/asakusa.dat");

        NullWritable key = NullWritable.get();
        AsakusaWritable1 val = new AsakusaWritable1();

        Option fileOpt = Writer.file(path);
        Option keyOpt = Writer.keyClass(key.getClass());
        Option valOpt = Writer.valueClass(val.getClass());
        Option compOpt = Writer.compression(CompressionType.NONE);
        try (Writer writer = SequenceFile.createWriter(conf, fileOpt, keyOpt, valOpt, compOpt)) {
            fill(val, 100);
            writer.append(key, val);
            fill(val, -1);
            writer.append(key, val);
            fill(val, 201);
            writer.append(key, val);
        }
    }

    @SuppressWarnings("deprecation")
    private static void fill(AsakusaWritable1 model, int value) {
        if (value < 0) {
            for (ValueOption<?> field : model.valueList) {
                field.setNull();
            }
            return;
        }

        model.booleanValue.modify((value % 2) == 0);
        model.byteValue.modify((byte) (value - 2));
        model.shortValue.modify((short) (value - 1));
        model.intValue.modify(value);
        model.longValue.modify(value + 1);
        model.floatValue.modify(value + 2);
        model.doubleValue.modify(value + 3);
        model.decimalValue.modify(BigDecimal.valueOf(value + 4));
        model.stringValue.modify(Integer.toString(value + 5));
        model.dateValue.modify(new Date(2023, 9, (value % 30) + 1));
        model.dateTimeValue.modify(new DateTime(2023, 9, (value % 30) + 1, 23, 59, value % 60));
    }

    static void writeWordCount() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("file://" + outputDir + "/wordcount.dat");

        Text key = new Text();
        IntWritable val = new IntWritable();

        Option fileOpt = Writer.file(path);
        Option keyOpt = Writer.keyClass(key.getClass());
        Option valOpt = Writer.valueClass(val.getClass());
        Option compOpt = Writer.compression(CompressionType.NONE);
        try (Writer writer = SequenceFile.createWriter(conf, fileOpt, keyOpt, valOpt, compOpt)) {
            key.set("Hello");
            val.set(11);
            writer.append(key, val);
            key.set("World");
            val.set(22);
            writer.append(key, val);
        }
    }
}
