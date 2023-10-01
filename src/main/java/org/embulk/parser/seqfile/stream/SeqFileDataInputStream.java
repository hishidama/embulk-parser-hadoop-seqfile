package org.embulk.parser.seqfile.stream;

import java.io.InputStream;

import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class SeqFileDataInputStream extends FSDataInputStream {

    public SeqFileDataInputStream(InputStream is) {
        super(new BufferedFSInputStream(new SeqFileFsInputStream(is), 8192));
    }
}
