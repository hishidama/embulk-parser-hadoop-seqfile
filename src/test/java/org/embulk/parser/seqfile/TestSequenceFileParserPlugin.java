package org.embulk.parser.seqfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.net.URL;
import java.text.ParseException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.hishidama.embulk.tester.EmbulkTestOutputPlugin.OutputRecord;
import com.hishidama.embulk.tester.EmbulkTestParserConfig;

public class TestSequenceFileParserPlugin {

    @Test
    public void wordcount() throws ParseException {
        try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
            tester.addParserPlugin(SequenceFileParserPlugin.TYPE, SequenceFileParserPlugin.class);

            EmbulkTestParserConfig parser = tester.newParserConfig(SequenceFileParserPlugin.TYPE);
            parser.set("key_class", Text.class.getName());
            parser.set("value_class", IntWritable.class.getName());
            parser.addColumn("word", "string").set("key", "true").set("wtype", "text");
            parser.addColumn("count", "long").set("key", "false").set("wtype", "int");

            URL inFile = getClass().getResource("wordcount.dat");
            List<OutputRecord> result = tester.runParser(inFile, parser);

            assertEquals(2, result.size());
            OutputRecord r0 = result.get(0);
            assertEquals("Hello", r0.getAsString("word"));
            assertEquals(11L, (long) r0.getAsLong("count"));
            OutputRecord r1 = result.get(1);
            assertEquals("World", r1.getAsString("word"));
            assertEquals(22L, (long) r1.getAsLong("count"));
        }
    }

    @Test
    public void example() throws ParseException {
        try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
            tester.addParserPlugin(SequenceFileParserPlugin.TYPE, SequenceFileParserPlugin.class);

            EmbulkTestParserConfig parser = tester.newParserConfig(SequenceFileParserPlugin.TYPE);
            parser.set("key_class", NullWritable.class.getName());
            parser.set("value_class", "com.example.hadoop.seqfile.ExampleWritable");
            parser.addColumn("nullValue", "long").set("key", "true").set("wtype", "null");
            parser.addColumn("booleanValue", "boolean").set("wtype", "boolean");
            parser.addColumn("byteValue", "long").set("wtype", "byte");
            parser.addColumn("shortValue", "long").set("wtype", "short");
            parser.addColumn("intValue", "long").set("wtype", "int");
            parser.addColumn("longValue", "long").set("wtype", "long");
            parser.addColumn("floatValue", "double").set("wtype", "float");
            parser.addColumn("doubleValue", "double").set("wtype", "double");
            parser.addColumn("vintValue", "long").set("wtype", "vint");
            parser.addColumn("vlongValue", "long").set("wtype", "vlong");
            parser.addColumn("stringValue", "string").set("wtype", "text");

            URL inFile = getClass().getResource("example.dat");
            List<OutputRecord> result = tester.runParser(inFile, parser);

            assertEquals(2, result.size());
            assertExample(100, result.get(0));
            assertExample(201, result.get(1));
        }
    }

    private static void assertExample(int expected, OutputRecord actual) {
        assertNull(actual.getAsLong("nullValue"));
        assertEquals((expected % 2) == 0, actual.getAsBoolean("booleanValue"));
        assertEquals((long) (byte) (expected - 2), (long) actual.getAsLong("byteValue"));
        assertEquals((long) (short) (expected - 1), (long) actual.getAsLong("shortValue"));
        assertEquals((long) expected, (long) actual.getAsLong("intValue"));
        assertEquals((long) (expected + 1), (long) actual.getAsLong("longValue"));
        assertEquals((double) (float) (expected + 2), (double) actual.getAsDouble("floatValue"), 0);
        assertEquals((double) (expected + 3), (double) actual.getAsDouble("doubleValue"), 0);
        assertEquals((long) (expected + 4), (long) actual.getAsLong("vintValue"));
        assertEquals((long) (expected + 5), (long) actual.getAsLong("vlongValue"));
        assertEquals(Integer.toString(expected + 6), actual.getAsString("stringValue"));
    }

    @Test
    public void asakusa() throws ParseException {
        try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
            tester.addParserPlugin(SequenceFileParserPlugin.TYPE, SequenceFileParserPlugin.class);

            EmbulkTestParserConfig parser = tester.newParserConfig(SequenceFileParserPlugin.TYPE);
            parser.set("value_class", "com.example.hadoop.seqfile.AsakusaWritable");
            parser.addColumn("booleanValue", "boolean").set("wtype", "booleanOption");
            parser.addColumn("byteValue", "long").set("wtype", "byteOption");
            parser.addColumn("shortValue", "long").set("wtype", "shortOption");
            parser.addColumn("intValue", "long").set("wtype", "intOption");
            parser.addColumn("longValue", "long").set("wtype", "longOption");
            parser.addColumn("floatValue", "double").set("wtype", "floatOption");
            parser.addColumn("doubleValue", "double").set("wtype", "doubleOption");
            parser.addColumn("decimalValue", "long").set("wtype", "decimalOption");
            parser.addColumn("stringValue", "string").set("wtype", "stringOption");
            parser.addColumn("dateValue", "timestamp").set("wtype", "dateOption");
            parser.addColumn("datetimeValue", "timestamp").set("wtype", "datetimeOption");

            URL inFile = getClass().getResource("asakusa.dat");
            List<OutputRecord> result = tester.runParser(inFile, parser);

            assertEquals(3, result.size());
            assertAsakusa(100, result.get(0));
            assertAsakusa(-1, result.get(1));
            assertAsakusa(201, result.get(2));
        }
    }

    private static void assertAsakusa(int expected, OutputRecord actual) {
        if (expected < 0) {
            assertNull(actual.getAsBoolean("booleanValue"));
            assertNull(actual.getAsLong("byteValue"));
            assertNull(actual.getAsLong("shortValue"));
            assertNull(actual.getAsLong("intValue"));
            assertNull(actual.getAsLong("longValue"));
            assertNull(actual.getAsDouble("floatValue"));
            assertNull(actual.getAsDouble("doubleValue"));
            assertNull(actual.getAsLong("decimalValue"));
            assertNull(actual.getAsString("stringValue"));
            assertNull(actual.getAsTimestamp("dateValue"));
            assertNull(actual.getAsTimestamp("datetimeValue"));
            return;
        }

        assertEquals((expected % 2) == 0, actual.getAsBoolean("booleanValue"));
        assertEquals((long) (byte) (expected - 2), (long) actual.getAsLong("byteValue"));
        assertEquals((long) (short) (expected - 1), (long) actual.getAsLong("shortValue"));
        assertEquals((long) expected, (long) actual.getAsLong("intValue"));
        assertEquals((long) (expected + 1), (long) actual.getAsLong("longValue"));
        assertEquals((double) (float) (expected + 2), (double) actual.getAsDouble("floatValue"), 0);
        assertEquals((double) (expected + 3), (double) actual.getAsDouble("doubleValue"), 0);
        assertEquals(BigDecimal.valueOf(expected + 4), BigDecimal.valueOf(actual.getAsLong("decimalValue")));
        assertEquals(Integer.toString(expected + 5), actual.getAsString("stringValue"));
        OffsetDateTime date = OffsetDateTime.of(2023, 9, (expected % 30) + 1, 0, 0, 0, 0, ZoneOffset.UTC);
        assertEquals(date.toInstant(), actual.getAsTimestamp("dateValue"));
        OffsetDateTime dateTime = OffsetDateTime.of(2023, 9, (expected % 30) + 1, 23, 59, expected % 60, 0, ZoneOffset.UTC);
        assertEquals(dateTime.toInstant(), actual.getAsTimestamp("datetimeValue"));
    }
}
