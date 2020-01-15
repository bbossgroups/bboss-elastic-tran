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
import org.frameworkset.spi.BaseApplicationContext;
import org.frameworkset.tran.hbase.input.HBaseTranException;
import org.frameworkset.util.concurrent.ThreadPoolFactory;

import java.util.Properties;
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
	public static TableFactory buildTableFactory(Properties properties){
		HbaseConfigurationFactoryBean hbaseConfigurationFactoryBean = new HbaseConfigurationFactoryBean();
		hbaseConfigurationFactoryBean.setProperties(properties);
		hbaseConfigurationFactoryBean.afterPropertiesSet();
		Configuration configuration = hbaseConfigurationFactoryBean.getConfiguration();
		final ExecutorService executorService = ThreadPoolFactory.buildThreadPool("HBase-Input","HBase tran",100,100,0L,1000l,1000,true,true);
		final ConnectionFactoryBean connectionFactoryBean = new ConnectionFactoryBean(configuration,executorService);
		try {
			TableFactory tableFactory = new HbaseTableFactory(connectionFactoryBean.getConnection());
			return tableFactory;
		} catch (Exception e) {
			throw new HBaseTranException(e);
		}
		finally {
			BaseApplicationContext.addShutdownHook(new Runnable() {
				@Override
				public void run() {
					try {
						connectionFactoryBean.destroy();
					} catch (Exception e) {

					}
					try {
						executorService.shutdown();
					}
					catch (Exception e){

					}

				}
			});
		}

	}
}
