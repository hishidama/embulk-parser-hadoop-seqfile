package org.embulk.parser.seqfile;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Writable;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.parser.seqfile.column.WritableColumnFactory;
import org.embulk.parser.seqfile.column.WritableType;
import org.embulk.parser.seqfile.processor.WritableProcessor;
import org.embulk.parser.seqfile.processor.WritableProcessorFactory;
import org.embulk.parser.seqfile.stream.SeqFileDataInputStream;
import org.embulk.parser.seqfile.writable.EmbulkWritableFactory;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.file.FileInputInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceFileParserPlugin implements ParserPlugin {
    private final Logger log = LoggerFactory.getLogger(getClass());

    public static final String TYPE = "hadoop_seqfile";

    public interface PluginTask extends Task, TimestampParserTask {
        @Config("key_class")
        @ConfigDefault("null")
        public Optional<String> getKeyClass();

        @Config("value_class")
        @ConfigDefault("null")
        public Optional<String> getValueClass();

        @Config("columns")
        public SchemaConfig getColumns();

        @Config("flush_count")
        @ConfigDefault("100")
        public int getFlushCount();
    }

    // From org.embulk.spi.time.TimestampParser.Task
    public interface TimestampParserTask {
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public String getDefaultTimeZoneId();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        public String getDefaultTimestampFormat();

        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        public String getDefaultDate();
    }

    public interface ColumnOptionTask extends Task, TimestampColumnOption {
        @Config("key")
        @ConfigDefault("false")
        public boolean getKey();

        @Config("wtype")
        @ConfigDefault("null")
        public Optional<WritableType> getWritableType();
    }

    // From org.embulk.spi.util.Timestamps.ParserTimestampColumnOption
    public interface TimestampColumnOption extends Task {
        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        public Optional<String> getDate();
    }

    protected static final ConfigMapper CONFIG_MAPPER;
    protected static final TaskMapper TASK_MAPPER;
    static {
        ConfigMapperFactory factory = ConfigMapperFactory.builder().addDefaultModules().build();
        CONFIG_MAPPER = factory.createConfigMapper();
        TASK_MAPPER = factory.createTaskMapper();
    }

    protected WritableColumnFactory writableColumnFactory = new WritableColumnFactory();
    protected WritableProcessorFactory writableProcessorFactory = new WritableProcessorFactory();

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control) {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        control.run(task.toTaskSource(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema, FileInput input, PageOutput output) {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        String keyClassName = task.getKeyClass().orElse(NullWritable.class.getName());
        log.info("config.keyClassName:   {}", keyClassName);
        Writable key = EmbulkWritableFactory.createWritable(keyClassName);
        String valueClassName = task.getValueClass().orElse(NullWritable.class.getName());
        log.info("config.valueClassName: {}", valueClassName);
        Writable value = EmbulkWritableFactory.createWritable(valueClassName);

        try (FileInputInputStream is = new FileInputInputStream(input)) {
            while (is.nextFile()) {
                try (SeqFileDataInputStream dis = new SeqFileDataInputStream(is)) {
                    Configuration conf = new Configuration();
                    conf.setClassLoader(EmbulkWritableFactory.getClassLoader());
                    Option streamOption = Reader.stream(dis);
                    try (Reader reader = new Reader(conf, streamOption)) {
                        run(task, schema, reader, key, value, output);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
            }
        }
    }

    protected void run(PluginTask task, Schema schema, Reader reader, Writable key, Writable value, PageOutput output) throws IOException {
        List<WritableColumn> keyColumnList = new ArrayList<>();
        List<WritableColumn> valueColumnList = new ArrayList<>();
        {
            List<ColumnConfig> configList = task.getColumns().getColumns();
            for (Column column : schema.getColumns()) {
                ColumnConfig config = configList.get(column.getIndex());
                ColumnOptionTask option = CONFIG_MAPPER.map(config.getOption(), ColumnOptionTask.class);
                WritableColumn writableColumn = writableColumnFactory.createColumn(task, column, option);
                if (option.getKey()) {
                    keyColumnList.add(writableColumn);
                } else {
                    valueColumnList.add(writableColumn);
                }
            }
        }

        WritableProcessor keyProcessor = writableProcessorFactory.createProcessor("key", key, keyColumnList);
        WritableProcessor valueProcessor = writableProcessorFactory.createProcessor("value", value, valueColumnList);

        try (PageBuilder pageBuilder = Exec.getPageBuilder(Exec.getBufferAllocator(), schema, output)) {

            int count = 0;
            final int flushCount = task.getFlushCount();
            for (;;) {
                boolean hasNext;
                try {
//                  hasNext = reader.next(key, value);
                    hasNext = reader.next(key);
                } catch (EOFException e) {
                    log.trace("EOFException", e);
                    hasNext = false;
                }
                if (!hasNext) {
                    pageBuilder.flush();
                    break;
                }
                reader.getCurrentValue(value);

                keyProcessor.writeTo(pageBuilder);
                valueProcessor.writeTo(pageBuilder);
                pageBuilder.addRecord();

                if (++count >= flushCount) {
                    log.trace("flush");
                    pageBuilder.flush();
                    count = 0;
                }
            }
            pageBuilder.finish();
        }
    }
}
