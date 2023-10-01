package org.embulk.parser.seqfile.writable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

// https://www.ne.jp/asahi/hishidama/home/tech/java/JavaCompiler.html
public class JavaClassObject extends SimpleJavaFileObject {

    public JavaClassObject(String name, Kind kind) {
        super(URI.create("string:///" + name.replace('.', '/') + kind.extension), kind);
    }

    protected final ByteArrayOutputStream bos = new ByteArrayOutputStream();

    // 実際の出力用ストリームを返す
    @Override
    public OutputStream openOutputStream() throws IOException {
        return bos;
    }

    // コンパイルされたバイトコード
    public byte[] getBytes() {
        return bos.toByteArray();
    }

    /** ロードされたクラス */
    private Class<?> clazz = null;

    public void setDefinedClass(Class<?> c) {
        clazz = c;
    }

    public Class<?> getDefinedClass() {
        return clazz;
    }
}
