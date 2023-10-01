package org.embulk.parser.seqfile.writable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class EmbulkWritableFactoryTest {

    @Test
    public void createNullWritable() {
        String className = "org.apache.hadoop.io.NullWritable";
        Writable writable = EmbulkWritableFactory.createWritable(className);

        assertEquals(NullWritable.class, writable.getClass());
        assertTrue(writable instanceof NullWritable);
    }

    @Test
    public void createIntWritable() {
        String className = "org.apache.hadoop.io.IntWritable";
        Writable writable = EmbulkWritableFactory.createWritable(className);

        assertEquals(IntWritable.class, writable.getClass());
        assertTrue(writable instanceof IntWritable);
    }

    @Test
    public void createEmbulkWritable() {
        testCreateEmbulkWritable("com.example.hadoop.seqfile.AsakusaWritable");
        testCreateEmbulkWritable("com.example.hadoop.seqfile.ExampleWritable");
        testCreateEmbulkWritable("com.example.hadoop.seqfile.WordCountWritable");
    }

    private void testCreateEmbulkWritable(String className) {
        Writable writable = EmbulkWritableFactory.createWritable(className);

        assertEquals(className, writable.getClass().getName());
        if (writable instanceof EmbulkWritable) {
            // success
        } else {
            fail(className + " is not EmbulkWritable");
        }
    }
}
