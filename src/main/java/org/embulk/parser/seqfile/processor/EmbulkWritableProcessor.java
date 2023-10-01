package org.embulk.parser.seqfile.processor;

import java.util.List;

import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.parser.seqfile.writable.EmbulkWritable;
import org.embulk.spi.PageBuilder;

public class EmbulkWritableProcessor implements WritableProcessor {

    private EmbulkWritable writable;

    public EmbulkWritableProcessor(EmbulkWritable writable, List<WritableColumn> columnList) {
        this.writable = writable;
        writable.setColumnList(columnList);
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        writable.writeTo(pageBuilder);
    }
}
