package org.embulk.parser.seqfile.processor;

import org.embulk.spi.PageBuilder;

public class NullWritableProcessor implements WritableProcessor {

    public NullWritableProcessor() {
    }

    @Override
    public void writeTo(PageBuilder pageBuilder) {
    }
}
