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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * HTableInterfaceFactory based on HTablePool.
 * @author emeroad
 * @author minwoo.jung
 * @author HyunGil Jeong
 */
public class HbaseTableFactory implements TableFactory {

    private final Connection connection;

    public HbaseTableFactory(Connection connection) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
    }

    @Override
    public Table getTable(TableName tableName) {
        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            throw new HbaseSystemException(e);
        }
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService executorService) {
        try {
            return connection.getTable(tableName, executorService);
        } catch (IOException e) {
            throw new HbaseSystemException(e);
        }
    }

    @Override
    public void releaseTable(Table table) {
        if (table == null) {
            return;
        }

        try {
            table.close();
        } catch (IOException ex) {
            throw new HbaseSystemException(ex);
        }
    }
}
