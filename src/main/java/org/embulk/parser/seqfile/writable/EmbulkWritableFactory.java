package org.embulk.parser.seqfile.writable;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbulkWritableFactory {
    private static final Logger log = LoggerFactory.getLogger(EmbulkWritableFactory.class);

    private static final String WRITABLE_PACKAGE = "package {packageName};";
    private static final String WRITABLE_CLASS = "public class {simpleName} extends " + EmbulkWritable.class.getName() + " {}";

    private static final JavaCompiler compiler;
    private static final ClassFileManager manager;
    static {
        compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new UnsupportedOperationException(MessageFormat.format("This java({0}) does not support ToolProvider.getSystemJavaCompiler()", System.getProperty("java.home")));
        }
        manager = new ClassFileManager(compiler, null);
    }

    public static Writable createWritable(String className) {
        if (className.equals(NullWritable.class.getName())) {
            return NullWritable.get();
        }

        try {
            Class<?> clazz = Class.forName(className, true, Writable.class.getClassLoader());
            return newInstance(clazz);
        } catch (ClassNotFoundException ignore) {
            // fall through
        }

        Class<? extends EmbulkWritable> clazz = getWritableClass(className);
        return newInstance(clazz);
    }

    protected static Writable newInstance(Class<?> clazz) {
        try {
            return Writable.class.cast(clazz.getConstructor().newInstance());
        } catch (IllegalArgumentException | ReflectiveOperationException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends EmbulkWritable> getWritableClass(String className) {
        String sourceCode = getWritableSourceCode(className);
        JavaFileObject sourceFile = new JavaSourceFromString(className, sourceCode);

        List<JavaFileObject> compilationUnits = Arrays.asList(sourceFile);
        String classPath = getClassPath();
        log.debug("EmbulkWritable classpath: " + classPath);
        List<String> options = Arrays.asList("-classpath", classPath);
        CompilationTask task = compiler.getTask(null, manager, null, options, null, compilationUnits);

        boolean successCompile = task.call();
        if (!successCompile) {
            throw new RuntimeException("Writable compile failed. className=" + className);
        }

        ClassLoader classLoader = manager.getClassLoader(null);
        try {
            return (Class<? extends EmbulkWritable>) classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getWritableSourceCode(String className) {
        String packageName, simpleName;
        {
            int n = className.lastIndexOf('.');
            if (n < 0) {
                packageName = null;
                simpleName = className;
            } else {
                packageName = className.substring(0, n);
                simpleName = className.substring(n + 1);
            }
        }

        String source;
        if (packageName == null) {
            source = "";
        } else {
            source = WRITABLE_PACKAGE.replace("{packageName}", packageName);
        }
        source += WRITABLE_CLASS.replace("{simpleName}", simpleName);

        return source;
    }

    private static String CLASS_PATH = null;

    private static String getClassPath() {
        if (CLASS_PATH == null) {
            ClassLoader classLoader = EmbulkWritable.class.getClassLoader();
            if (classLoader instanceof URLClassLoader) {
                @SuppressWarnings("resource")
                URLClassLoader urlClassLoader = (URLClassLoader) classLoader;

                StringBuilder sb = new StringBuilder(1024 * 11);
                for (URL url : urlClassLoader.getURLs()) {
                    if (sb.length() != 0) {
                        sb.append(File.pathSeparator);
                    }
                    try {
                        sb.append(Paths.get(url.toURI()));
                    } catch (URISyntaxException ignore) {
                        log.debug("ignore classLoader URI", ignore);
                    }
                }
                CLASS_PATH = sb.toString();
            } else {
                CLASS_PATH = System.getProperty("java.class.path");
            }
        }
        return CLASS_PATH;
    }

    public static ClassLoader getClassLoader() {
        return manager.getClassLoader(null);
    }
}
