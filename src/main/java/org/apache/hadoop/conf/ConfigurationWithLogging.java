package org.apache.hadoop.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs access to {@link Configuration}.
 * Sensitive data will be redacted.
 */
@InterfaceAudience.Private
public class ConfigurationWithLogging extends Configuration {
    private static final Logger LOG =
            LoggerFactory.getLogger(ConfigurationWithLogging.class);

    private final Logger log;
    private final ConfigRedactor redactor;

    public ConfigurationWithLogging(Configuration conf) {
        super(conf);
        log = LOG;
        redactor = new ConfigRedactor(conf);
    }

    /**
     * @see Configuration#get(String).
     */
    @Override
    public String get(String name) {
        String value = super.get(name);
        log.info("Got {} = '{}'", name, redactor.redact(name, value));
        return value;
    }

    /**
     * @see Configuration#get(String, String).
     */
    @Override
    public String get(String name, String defaultValue) {
        String value = super.get(name, defaultValue);
        log.info("Got "+ name +" = '{}' (default '{}')",
                redactor.redact(name, value), redactor.redact(name, defaultValue));
        return value;
    }

    /**
     * @see Configuration#getBoolean(String, boolean).
     */
    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        boolean value = super.getBoolean(name, defaultValue);
        log.info("Got "+ name +" = '{}' (default '{}')", value, defaultValue);
        return value;
    }

    /**
     * @see Configuration#getFloat(String, float).
     */
    @Override
    public float getFloat(String name, float defaultValue) {
        float value = super.getFloat(name, defaultValue);
        log.info("Got "+ name +" = '{}' (default '{}')", value, defaultValue);
        return value;
    }

    /**
     * @see Configuration#getInt(String, int).
     */
    @Override
    public int getInt(String name, int defaultValue) {
        int value = super.getInt(name, defaultValue);
        log.info("Got "+ name +" = '{}' (default '{}')", value, defaultValue);
        return value;
    }

    /**
     * @see Configuration#getLong(String, long).
     */
    @Override
    public long getLong(String name, long defaultValue) {
        long value = super.getLong(name, defaultValue);
        log.info("Got "+ name +" = '{}' (default '{}')", value, defaultValue);
        return value;
    }

    /**
     * @see Configuration#set(String, String, String).
     */
    @Override
    public void set(String name, String value, String source) {
        log.info("Set "+ name +" to '{}'{}", redactor.redact(name, value),
                source == null ? "" : " from " + source);
        super.set(name, value, source);
    }
}