package com.example.hadoop.seqfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

class WordCountWritable1 implements Writable {

    private Text word = new Text();
    private IntWritable count = new IntWritable();

    public void setWord(String s) {
        word.set(s);
    }

    public String getWord() {
        return word.toString();
    }

    public void setCount(int n) {
        count.set(n);
    }

    public int getCount() {
        return count.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        count.readFields(in);
    }
}
