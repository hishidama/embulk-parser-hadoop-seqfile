package com.example.hadoop.seqfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public class ExampleWritable1 implements Writable {

    public final BooleanWritable booleanValue = new BooleanWritable();
    public final ByteWritable byteValue = new ByteWritable();
    public final ShortWritable shortValue = new ShortWritable();
    public final IntWritable intValue = new IntWritable();
    public final LongWritable longValue = new LongWritable();
    public final FloatWritable floatValue = new FloatWritable();
    public final DoubleWritable doubleValue = new DoubleWritable();
    public final VIntWritable vintValue = new VIntWritable();
    public final VLongWritable vlongValue = new VLongWritable();
    public final Text stringValue = new Text();

    public final List<Writable> valueList = Arrays.asList( //
            booleanValue, //
            byteValue, //
            shortValue, //
            intValue, //
            longValue, //
            floatValue, //
            doubleValue, //
            vintValue, //
            vlongValue, //
            stringValue);

    @Override
    public void write(DataOutput out) throws IOException {
        for (Writable value : valueList) {
            value.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (Writable value : valueList) {
            value.readFields(in);
        }
    }
}
