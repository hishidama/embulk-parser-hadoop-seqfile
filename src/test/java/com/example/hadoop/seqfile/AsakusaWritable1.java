package com.example.hadoop.seqfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.value.*;

public class AsakusaWritable1 implements Writable {

    public final BooleanOption booleanValue = new BooleanOption();
    public final ByteOption byteValue = new ByteOption();
    public final ShortOption shortValue = new ShortOption();
    public final IntOption intValue = new IntOption();
    public final LongOption longValue = new LongOption();
    public final FloatOption floatValue = new FloatOption();
    public final DoubleOption doubleValue = new DoubleOption();
    public final DecimalOption decimalValue = new DecimalOption();
    public final StringOption stringValue = new StringOption();
    public final DateOption dateValue = new DateOption();
    public final DateTimeOption dateTimeValue = new DateTimeOption();

    public final List<ValueOption<?>> valueList = Arrays.asList( //
            booleanValue, //
            byteValue, //
            shortValue, //
            intValue, //
            longValue, //
            floatValue, //
            doubleValue, //
            decimalValue, //
            stringValue, //
            dateValue, //
            dateTimeValue);

    @Override
    public void write(DataOutput out) throws IOException {
        for (ValueOption<?> value : valueList) {
            value.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (ValueOption<?> value : valueList) {
            value.readFields(in);
        }
    }
}
