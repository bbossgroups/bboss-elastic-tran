/*
 * Copyright 2018 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.frameworkset.nosql.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.frameworkset.spi.DisposableBean;
import org.frameworkset.spi.InitializingBean;
import org.frameworkset.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author emeroad
 * @author HyunGil Jeong
 * @author minwoo.jung
 */
public class HbaseTemplate2 extends HbaseAccessor implements InitializingBean, DisposableBean {

    private static final int DEFAULT_MAX_THREADS_FOR_PARALLEL_SCANNER = 128;
    private static final int DEFAULT_MAX_THREADS_PER_PARALLEL_SCAN = 1;

    private static final long DEFAULT_DESTORY_TIMEOUT = 2000;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final boolean debugEnabled = this.logger.isDebugEnabled();

    private final AtomicBoolean isClose = new AtomicBoolean(false);

    private ExecutorService executor;
    private boolean enableParallelScan = false;
    private int maxThreads = DEFAULT_MAX_THREADS_FOR_PARALLEL_SCANNER;
    private int maxThreadsPerParallelScan = DEFAULT_MAX_THREADS_PER_PARALLEL_SCAN;

    private HBaseAsyncOperation asyncOperation = DisabledHBaseAsyncOperation.INSTANCE;

    public HbaseTemplate2() {
    }
    
    private Table getTable(TableName tableName) {
        return getTableFactory().getTable(tableName);
    }
    
    public void setEnableParallelScan(boolean enableParallelScan) {
        this.enableParallelScan = enableParallelScan;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public void setMaxThreadsPerParallelScan(int maxThreadsPerParallelScan) {
        this.maxThreadsPerParallelScan = maxThreadsPerParallelScan;
    }

    public void setAsyncOperation(HBaseAsyncOperation asyncOperation) {
        if (asyncOperation == null) {
            throw new NullPointerException("asyncOperation");
        }
        this.asyncOperation = asyncOperation;
    }

    
    public void afterPropertiesSet() {
        Configuration configuration = getConfiguration();
        Assert.notNull(configuration, "configuration is required");
        Assert.notNull(getTableFactory(), "tableFactory is required");

    }

    
    public void destroy() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        if (isClose.compareAndSet(false, true)) {
            logger.info("HBaseTemplate2.destroy()");
            final ExecutorService executor = this.executor;
            if (executor != null) {
                executor.shutdown();
                try {
                    executor.awaitTermination(DEFAULT_DESTORY_TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            long remainingTime = Math.max(DEFAULT_DESTORY_TIMEOUT - stopWatch.stop(), 100);
            awaitAsyncPutOpsCleared(remainingTime, 50);
        }
    }

    private boolean awaitAsyncPutOpsCleared(long waitTimeout, long checkUnitTime) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        while (true) {
            Long currentPutOpsCount = asyncOperation.getCurrentOpsCount();
            if (logger.isWarnEnabled()) {
                logger.warn("count {}", currentPutOpsCount);
            }

            if (currentPutOpsCount <= 0L) {
                return true;
            }

            if (stopWatch.stop() > waitTimeout) {
                return false;
            }
            try {
                Thread.sleep(checkUnitTime);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void assertAccessAvailable() {
        if (isClose.get()) {
            throw new HBaseAccessException("Already closed.");
        }
    }


    public void put(TableName tableName, final byte[] rowName, final byte[] familyName, final byte[] qualifier, final byte[] value) {
        put(tableName, rowName, familyName, qualifier, null, value);
    }

    public void put(TableName tableName, final byte[] rowName, final byte[] familyName, final byte[] qualifier, final Long timestamp, final byte[] value) {
        assertAccessAvailable();
        execute(tableName, new TableCallback() {
            
            public Object doInTable(Table table) throws Throwable {
                Put put = createPut(rowName, familyName, timestamp, qualifier, value);
                table.put(put);
                return null;
            }
        });
    }


    public void put(TableName tableName, final Put put) {
        assertAccessAvailable();
        execute(tableName, new TableCallback() {
            
            public Object doInTable(Table table) throws Throwable {
                table.put(put);
                return null;
            }
        });
    }

    public void put(TableName tableName, final List<Put> puts) {
        assertAccessAvailable();
        execute(tableName, new TableCallback() {
            
            public Object doInTable(Table table) throws Throwable {
                table.put(puts);
                return null;
            }
        });
    }

    public boolean asyncPut(TableName tableName, byte[] rowName, byte[] familyName, byte[] qualifier, byte[] value) {
        return asyncPut(tableName, rowName, familyName, qualifier, null, value);
    }

    public boolean asyncPut(TableName tableName, byte[] rowName, byte[] familyName, byte[] qualifier, Long timestamp, byte[] value) {
        Put put = createPut(rowName, familyName, timestamp, qualifier, value);
        return asyncPut(tableName, put);
    }


    public boolean asyncPut(TableName tableName, Put put) {
        assertAccessAvailable();
        if (asyncOperation.isAvailable()) {
            return asyncOperation.put(tableName, put);
        } else {
            put(tableName, put);
            return true;
        }
    }

    public List<Put> asyncPut(TableName tableName, List<Put> puts) {
        assertAccessAvailable();
        if (asyncOperation.isAvailable()) {
            return asyncOperation.put(tableName, puts);
        } else {
            put(tableName, puts);
            return Collections.emptyList();
        }
    }

    private Put createPut(byte[] rowName, byte[] familyName, Long timestamp, byte[] qualifier, byte[] value) {
        Put put = new Put(rowName);
        if (familyName != null) {
            if (timestamp == null) {
                put.addColumn(familyName, qualifier, value);
            } else {
                put.addColumn(familyName, qualifier, timestamp, value);
            }
        }
        return put;
    }

    public void delete(TableName tableName, final Delete delete) {
        assertAccessAvailable();
        execute(tableName, new TableCallback() {
            
            public Object doInTable(Table table) throws Throwable {
                table.delete(delete);
                return null;
            }
        });
    }

    public void delete(TableName tableName, final List<Delete> deletes) {
        assertAccessAvailable();
        execute(tableName, new TableCallback() {
            
            public Object doInTable(Table table) throws Throwable {
                table.delete(deletes);
                return null;
            }
        });
    }


    
    public Result increment(TableName tableName, final Increment increment) {
        assertAccessAvailable();
        return execute(tableName, new TableCallback<Result>() {
            
            public Result doInTable(Table table) throws Throwable {
                return table.increment(increment);
            }
        });
    }

    
    public List<Result> increment(final TableName tableName, final List<Increment> incrementList) {
        assertAccessAvailable();
        return execute(tableName, new TableCallback<List<Result>>() {
            
            public List<Result> doInTable(Table table) throws Throwable {
                final List<Result> resultList = new ArrayList<Result>(incrementList.size());

                Exception lastException = null;
                for (Increment increment : incrementList) {
                    try {
                        Result result = table.increment(increment);
                        resultList.add(result);
                    } catch (IOException e) {
                        logger.warn("{} increment error Caused:{}", tableName, e.getMessage(), e);
                        lastException = e;
                    }
                }
                if (lastException != null) {
                    throw lastException;
                }
                return resultList;
            }
        });
    }

    public long incrementColumnValue(TableName tableName, final byte[] rowName, final byte[] familyName, final byte[] qualifier, final long amount) {
        assertAccessAvailable();
        return execute(tableName, new TableCallback<Long>() {
            
            public Long doInTable(Table table) throws Throwable {
                return table.incrementColumnValue(rowName, familyName, qualifier, amount);
            }
        });
    }

    public long incrementColumnValue(TableName tableName, final byte[] rowName, final byte[] familyName, final byte[] qualifier, final long amount, final boolean writeToWAL) {
        assertAccessAvailable();
        return execute(tableName, new TableCallback<Long>() {
            
            public Long doInTable(Table table) throws Throwable {
                return table.incrementColumnValue(rowName, familyName, qualifier, amount, writeToWAL? Durability.SKIP_WAL: Durability.USE_DEFAULT);
            }
        });
    }
    
    public <T> T execute(TableName tableName, TableCallback<T> action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");
        assertAccessAvailable();

        Table table = getTable(tableName);

        try {
            T result = action.doInTable(table);
            return result;
        } catch (Throwable e) {
            if (e instanceof Error) {
                throw ((Error) e);
            }
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            }
            throw new HbaseSystemException((Exception) e);
        } finally {
            releaseTable(table);
        }
    }
    
    private void releaseTable(Table table) {
        getTableFactory().releaseTable(table);
    }

}
