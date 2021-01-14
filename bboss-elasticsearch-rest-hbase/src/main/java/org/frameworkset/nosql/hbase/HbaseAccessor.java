/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.frameworkset.nosql.hbase;

import org.apache.hadoop.conf.Configuration;

import java.nio.charset.Charset;

/**
 * Base class for {@link HbaseTemplate2} and {HbaseInterceptor}, defining commons properties such as {HTableInterfaceFactory} and {@link Configuration}.
 * 
 * Not intended to be used directly.
 * 
 * @author Costin Leau
 */
public abstract class HbaseAccessor {

    private String encoding;
    private static final Charset CHARSET = Charsets.UTF_8;

    private TableFactory tableFactory;
    private Configuration configuration;

    /**
     * Sets the table factory.
     *
     * @param tableFactory The tableFactory to set.
     */
    public void setTableFactory(TableFactory tableFactory) {
        this.tableFactory = tableFactory;
    }

    /**
     * Sets the encoding.
     *
     * @param encoding The encoding to set.
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Sets the configuration.
     *
     * @param configuration The configuration to set.
     */
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public TableFactory getTableFactory() {
        return tableFactory;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
    
    public Charset getCharset() {
        return (hasText(encoding) ? Charset.forName(encoding) : CHARSET);
    }
    /**
     * Check whether the given {@code String} contains actual <em>text</em>.
     * <p>More specifically, this method returns {@code true} if the
     * {@code String} is not {@code null}, its length is greater than 0,
     * and it contains at least one non-whitespace character.
     * @param str the {@code String} to check (may be {@code null})
     * @return {@code true} if the {@code String} is not {@code null}, its
     * length is greater than 0, and it does not contain whitespace only
     */
    public static boolean hasText(String str) {
        return (hasLength(str) && containsText(str));
    }

    private static boolean containsText(CharSequence str) {
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }
    /**
     * Check that the given {@code String} is neither {@code null} nor of length 0.
     * <p>Note: this method returns {@code true} for a {@code String} that
     * purely consists of whitespace.
     * @param str the {@code String} to check (may be {@code null})
     * @return {@code true} if the {@code String} is not {@code null} and has length
     * @see #hasText(String)
     */
    public static boolean hasLength(String str) {
        return (str != null && !str.isEmpty());
    }

}