package org.embulk.parser.seqfile.column;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum WritableType {

    // Hadoop Writable
    NULL, BOOLEAN, //
    BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, //
    VINT, VLONG, //
    TEXT, //

    // Asakusa Framework
    BOOLEAN_OPTION, //
    BYTE_OPTION, SHORT_OPTION, INT_OPTION, LONG_OPTION, FLOAT_OPTION, DOUBLE_OPTION, DECIMAL_OPTION, //
    STRING_OPTION, //
    DATE_OPTION, DATETIME_OPTION, //

    //
    ;

    private final String name;

    WritableType() {
        String[] ss = name().split("_");
        StringBuilder sb = new StringBuilder();
        sb.append(ss[0].toLowerCase(Locale.ENGLISH));
        for (int i = 1; i < ss.length; i++) {
            String s = ss[i];
            sb.append(Character.toUpperCase(s.charAt(0)));
            sb.append(s.substring(1).toLowerCase(Locale.ENGLISH));
        }
        this.name = sb.toString();
    }

    @JsonValue
    @Override
    public String toString() {
        return this.name;
    }

    private static final Map<String, WritableType> MAP;
    static {
        Map<String, WritableType> map = new HashMap<>();
        for (WritableType type : values()) {
            map.put(type.toString().toLowerCase(Locale.ENGLISH), type);
            map.put(type.name().toLowerCase(Locale.ENGLISH), type);
        }
        MAP = map;
    }

    @JsonCreator
    public static WritableType fromString(String value) {
        WritableType type = MAP.get(value.toLowerCase(Locale.ENGLISH));
        if (type != null) {
            return type;
        }
        throw new ConfigException(MessageFormat.format("Unknown wtype ''{0}''. Supported types are {1}", //
                value, Arrays.stream(values()).map(WritableType::toString).sorted().collect(Collectors.joining(", "))));
    }
}
