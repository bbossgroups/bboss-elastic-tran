package org.frameworkset.nosql.hbase;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.frameworkset.tran.hbase.HBaseTranException;
import org.frameworkset.util.concurrent.ThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/1/4 17:50
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseHelper {
	private static Logger logger = LoggerFactory.getLogger(HBaseHelper.class);
	private ConnectionFactoryBean connectionFactoryBean;
	private ExecutorService executorService;
	private TableFactory tableFactory;
	private Admin admin;
	public void buildHBaseClient(Map<String,String> properties,int threadCount,int threadQueue,long keepAliveTime,
												 long blockedWaitTimeout,int warnMultsRejects,boolean preStartAllCoreThreads,final Boolean daemon){
		if(connectionFactoryBean == null) {
			if(connectionFactoryBean != null){
				return;
			}
			synchronized (HBaseHelper.class) {
				HbaseConfigurationFactoryBean hbaseConfigurationFactoryBean = new HbaseConfigurationFactoryBean();
				hbaseConfigurationFactoryBean.setProperties(properties);
				hbaseConfigurationFactoryBean.afterPropertiesSet();
				Configuration configuration = hbaseConfigurationFactoryBean.getConfiguration();
//		final ExecutorService executorService = ThreadPoolFactory.buildThreadPool("HBase-Input","HBase tran",100,100,0L,1000l,1000,true,true);
				executorService = ThreadPoolFactory.buildThreadPool("HBase-Input", "HBase tran", threadCount, threadQueue, keepAliveTime,
						blockedWaitTimeout, warnMultsRejects, preStartAllCoreThreads, daemon);
				connectionFactoryBean = new ConnectionFactoryBean(configuration, executorService);

				try {
					TableFactory tableFactory = new HbaseTableFactory(connectionFactoryBean.getConnection());

					this.tableFactory = tableFactory;
					admin = connectionFactoryBean.getConnection().getAdmin();
//			HBaseAsyncOperationMetrics hBaseAsyncOperationMetrics = new HBaseAsyncOperationMetrics(hBaseAsyncOperation);
				} catch (Exception e) {
					throw new HBaseTranException(e);
				} finally {
//					try {
//						ShutdownHookManagerProxy shutdownHookManagerProxy = new ShutdownHookManagerProxy();
////						shutdownHookManagerProxy.destroy();
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//
				}
			}
		}

	}
    public TableFactory getTableFactory(){
		return tableFactory;
	}

	public void destroy(){
		try {
			if(admin != null){
				admin.close();
			}
		}
		catch (Throwable e) {
				logger.warn("",e);
		}
		try {
			if(connectionFactoryBean != null) {
				connectionFactoryBean.destroy();
				connectionFactoryBean = null;
			}
		} catch (Throwable e) {
			logger.warn("",e);
		}
		try {
			if(executorService != null) {
                ThreadPoolFactory.shutdownExecutor(executorService);
				executorService = null;
			}
		}
		catch (Throwable e){
			logger.warn("",e);
		}


		tableFactory = null;


	}

	public Table getTable(String  tableName) {
		return getTableFactory().getTable(TableName.valueOf(tableName));
	}
	public void put(String tableName, List<Put> puts){
		Table table = null;
		try{
			table = getTable(tableName);
			table.put(puts);
		} catch (Exception e) {
			throw new HBaseAccessException("put datas to " + tableName +" failed:",e);
		} finally {
			if(table != null){
				releaseTable(table);
			}
		}


	}

	public void put(String tableName, Put puts){
		Table table = null;
		try{
			table = getTable(tableName);
			table.put(puts);
		} catch (Exception e) {
			throw new HBaseAccessException("put datas to " + tableName +" failed:",e);
		} finally {
			if(table != null){
				releaseTable(table);
			}
		}


	}
	public void releaseTable(Table table){
		getTableFactory().releaseTable(table);
	}

	public void createTable(String tableName, String[] colFamily)
	{
		try {


			TableName tableName_ = TableName.valueOf(tableName);
			if (admin.tableExists(tableName_)) {
				logger.warn(tableName + "表已经存在");
			} else {
				HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName_);
				for (String str : colFamily) {
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
					hTableDescriptor.addFamily(hColumnDescriptor);
					admin.createTable(hTableDescriptor);
				}
			}
		}
		catch (Exception e){
			throw new HbaseSystemException("createTable "+tableName + " failed:",e);
		}
	}
}
