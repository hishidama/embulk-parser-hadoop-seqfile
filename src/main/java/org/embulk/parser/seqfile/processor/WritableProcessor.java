package org.embulk.parser.seqfile.processor;

import org.embulk.spi.PageBuilder;

public interface WritableProcessor {
    public void writeTo(PageBuilder pageBuilder);
}
