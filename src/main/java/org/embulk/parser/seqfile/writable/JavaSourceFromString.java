package org.embulk.parser.seqfile.writable;

import java.net.URI;

import javax.tools.SimpleJavaFileObject;

// https://www.ne.jp/asahi/hishidama/home/tech/java/JavaCompiler.html
/**
 * 文字列に格納されたソースを表すために使用するファイルオブジェクト。
 */
public class JavaSourceFromString extends SimpleJavaFileObject {
    /**
     * この「ファイル」のソースコード。
     */
    protected final String code;

    /**
     * 新しい JavaSourceFromString を構築。
     * 
     * @param name このファイルオブジェクトで表されるコンパイルユニットのクラス名（FQCN）
     * @param code このファイルオブジェクトで表されるコンパイルユニットのソースコード
     */
    public JavaSourceFromString(String name, String code) {
        super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
        this.code = code;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
        return code;
    }
}
