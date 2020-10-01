package org.apache.hadoop.conf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT;

/**
 * Tool for redacting sensitive information when displaying config parameters.
 *
 * <p>Some config parameters contain sensitive information (for example, cloud
 * storage keys). When these properties are displayed in plaintext, we should
 * redactor their values as appropriate.
 */
public class ConfigRedactor {

    private static final String REDACTED_TEXT = "<redacted>";

    private List<Pattern> compiledPatterns;

    public ConfigRedactor(Configuration conf) {
        String sensitiveRegexList = conf.get(
                HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT);
        List<String> sensitiveRegexes =
                Arrays.asList(StringUtils.getTrimmedStrings(sensitiveRegexList));
        compiledPatterns = new ArrayList<Pattern>();
        for (String regex : sensitiveRegexes) {
            Pattern p = Pattern.compile(regex);
            compiledPatterns.add(p);
        }
    }

    /**
     * Given a key / value pair, decides whether or not to redact and returns
     * either the original value or text indicating it has been redacted.
     *
     * @param key
     * @param value
     * @return Original value, or text indicating it has been redacted
     */
    public String redact(String key, String value) {
        if (configIsSensitive(key)) {
            return REDACTED_TEXT;
        }
        return value;
    }

    /**
     * Matches given config key against patterns and determines whether or not
     * it should be considered sensitive enough to redact in logs and other
     * plaintext displays.
     *
     * @param key
     * @return True if parameter is considered sensitive
     */
    private boolean configIsSensitive(String key) {
        for (Pattern regex : compiledPatterns) {
            if (regex.matcher(key).find()) {
                return true;
            }
        }
        return false;
    }
}