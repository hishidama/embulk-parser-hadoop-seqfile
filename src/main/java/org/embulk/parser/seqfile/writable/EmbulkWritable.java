package org.embulk.parser.seqfile.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.embulk.parser.seqfile.column.WritableColumn;
import org.embulk.spi.PageBuilder;

public class EmbulkWritable implements Writable {

    private List<WritableColumn> columnList;

    public void setColumnList(List<WritableColumn> columnList) {
        this.columnList = columnList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("write() unsupported");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (WritableColumn column : columnList) {
            column.readFields(in);
        }
    }

    public void writeTo(PageBuilder pageBuilder) {
        for (WritableColumn column : columnList) {
            column.writeTo(pageBuilder);
        }
    }
}
