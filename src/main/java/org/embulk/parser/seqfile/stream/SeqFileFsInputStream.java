package org.embulk.parser.seqfile.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;

import org.apache.hadoop.fs.FSInputStream;

public class SeqFileFsInputStream extends FSInputStream {

    private final InputStream is;
    private long position = 0;

    public SeqFileFsInputStream(InputStream is) {
        this.is = is;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < this.position) {
            throw new UnsupportedOperationException(MessageFormat.format("unsupported seek operation. pos={0}, now={1}", pos, this.position));
        }

        while (this.position < pos) {
            if (read() < 0) {
                throw new EOFException();
            }
        }
    }

    @Override
    public long getPos() throws IOException {
        return this.position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        int len = is.read();
        if (len >= 0) {
            this.position++;
        }
        return len;
    }

    @Override
    public void close() throws IOException {
        is.close();
    }

    @Override
    public String toString() {
        return "TmpFsInputStream{position=" + position + "}";
    }
}
