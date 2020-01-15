/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.frameworkset.nosql.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * @author HyunGil Jeong
 */
public class ConnectionFactoryBean {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Connection connection;

    public ConnectionFactoryBean(Configuration configuration) {
        Objects.requireNonNull(configuration, " must not be null");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new HbaseSystemException(e);
        }
    }

    public ConnectionFactoryBean(Configuration configuration, ExecutorService executorService) {
        Objects.requireNonNull(configuration, "configuration must not be null");
        Objects.requireNonNull(executorService, "executorService must not be null");
        try {
            connection = ConnectionFactory.createConnection(configuration, executorService);
        } catch (IOException e) {
            throw new HbaseSystemException(e);
        }
    }

    public Connection getConnection() throws Exception {
        return connection;
    }

    public void destroy() throws Exception {
        logger.info("Hbase Connection destroy()");
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.warn("Hbase Connection.close() error: " + e.getMessage(), e);
            }
        }
    }
}
