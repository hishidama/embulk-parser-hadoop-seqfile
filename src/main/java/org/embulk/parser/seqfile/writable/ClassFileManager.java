package org.embulk.parser.seqfile.writable;

import java.io.IOException;
import java.security.SecureClassLoader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.tools.DiagnosticListener;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;

import org.apache.hadoop.io.Writable;

// https://www.ne.jp/asahi/hishidama/home/tech/java/JavaCompiler.html
public class ClassFileManager extends ForwardingJavaFileManager<JavaFileManager> {

    public ClassFileManager(JavaCompiler compiler, DiagnosticListener<? super JavaFileObject> listener) {
        super(compiler.getStandardFileManager(listener, null, null));
    }

    /** キー：クラス名、値：クラスファイルのオブジェクト */
    protected final Map<String, JavaClassObject> map = new ConcurrentHashMap<>();

    // クラスファイルを生成するときに呼ばれる
    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling) throws IOException {
        JavaClassObject classObject = new JavaClassObject(className, kind);
        map.put(className, classObject); // クラス名をキーにしてファイルオブジェクトを保持しておく
        return classObject;
    }

    protected ClassLoader loader = null;

    @Override
    public synchronized ClassLoader getClassLoader(Location location) {
        if (loader == null) {
            loader = new Loader();
        }
        return loader;
    }

    /** コンパイルしたクラスを返す為のクラスローダー */
    private class Loader extends SecureClassLoader {

//      public Loader() {
//          super(Writable.class.getClassLoader());
//      }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            JavaClassObject classObject = map.get(name);
            if (classObject == null) {
//              return super.findClass(name);
                return Writable.class.getClassLoader().loadClass(name);
            }

            synchronized (classObject) {
                Class<?> c = classObject.getDefinedClass();
                if (c == null) {
                    byte[] b = classObject.getBytes();
                    c = super.defineClass(name, b, 0, b.length);
                    classObject.setDefinedClass(c);
                }
                return c;
            }
        }
    }
}
