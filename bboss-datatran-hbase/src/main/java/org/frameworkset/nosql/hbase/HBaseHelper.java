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
import org.apache.hadoop.util.ShutdownHookManagerProxy;
import org.frameworkset.nosql.hbase.metrics.CollectorMetric;
import org.frameworkset.tran.hbase.HBaseTranException;
import org.frameworkset.util.concurrent.ThreadPoolFactory;
import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static ConnectionFactoryBean connectionFactoryBean;
	private static ExecutorService executorService;
	private static HbaseTemplate2 hbaseTemplate2;
	private static TableFactory tableFactory;
	private static CollectorMetric collectorMetric;
	public static void buildHBaseClient(Map<String,String> properties,int threadCount,int threadQueue,long keepAliveTime,
												 long blockedWaitTimeout,int warnMultsRejects,boolean preStartAllCoreThreads,final Boolean daemon,final Boolean enableMetrics){
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
					HBaseAsyncOperation hBaseAsyncOperation = HBaseAsyncOperationFactory.create(connectionFactoryBean.getConnection(), configuration);
					HbaseTemplate2 hbaseTemplate2 = new HbaseTemplate2();
					hbaseTemplate2.setConfiguration(configuration);
					hbaseTemplate2.setAsyncOperation(hBaseAsyncOperation);
					hbaseTemplate2.setTableFactory(tableFactory);
					hbaseTemplate2.afterPropertiesSet();
					HBaseHelper.hbaseTemplate2 = hbaseTemplate2;
					HBaseHelper.tableFactory = tableFactory;
					if (enableMetrics != null && enableMetrics) {
						collectorMetric = new CollectorMetric();
						collectorMetric.start(hBaseAsyncOperation);
					}
//			HBaseAsyncOperationMetrics hBaseAsyncOperationMetrics = new HBaseAsyncOperationMetrics(hBaseAsyncOperation);
				} catch (Exception e) {
					throw new HBaseTranException(e);
				} finally {
					try {
						ShutdownHookManagerProxy shutdownHookManagerProxy = new ShutdownHookManagerProxy();
//						shutdownHookManagerProxy.destroy();
					} catch (Exception e) {
						e.printStackTrace();
					}
					ShutdownUtil.addShutdownHook(new Runnable() {
						@Override
						public void run() {

							destroy();


						}
					});
				}
			}
		}

	}
    public static TableFactory getTableFactory(){
		return tableFactory;
	}
	public static HbaseTemplate2 getHbaseTemplate2(){
		return hbaseTemplate2;
	}
	public static void destroy(){

		try {
			if(hbaseTemplate2 != null) {
				hbaseTemplate2.destroy();
				hbaseTemplate2 = null;
			}
		}
		catch (Throwable e){
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
				executorService.shutdown();
				executorService = null;
			}
		}
		catch (Throwable e){
			logger.warn("",e);
		}
		if(collectorMetric != null) {
			collectorMetric.shutdown();
			collectorMetric = null;
		}
		tableFactory = null;


	}
}
