package org.embulk.parser.seqfile.processor;

import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.PageBuilder;

public class SimpleWritableProcessor implements WritableProcessor {

    private final WritableColumn writableColumn;

    public SimpleWritableProcessor(WritableColumn writableColumn) {
        this.writableColumn = writableColumn;
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
        writableColumn.writeTo(pageBuilder);
    }
}
