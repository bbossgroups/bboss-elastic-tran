package org.frameworkset.tran.schedule;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/4/13 12:59
 * @author biaoping.yin
 * @version 1.0
 */
public class ExternalScheduler {
	private DataStreamBuilder dataStreamBuilder;
	private DataStream dataStream;
	private static Logger logger = LoggerFactory.getLogger(ExternalScheduler.class);
//	private Lock lock = new ReentrantLock();
	public void dataStream(DataStreamBuilder dataStreamBuilder){
		this.dataStreamBuilder = dataStreamBuilder;
	}

	public void execute(Object params){
		if(dataStream == null) {
			try {
//				lock.lock();
				if(dataStream == null) {
					ImportBuilder db2ESImportBuilder = dataStreamBuilder.builder( params);
					if (!db2ESImportBuilder.isExternalTimer())//强制设置为外部定时器模式
						db2ESImportBuilder.setExternalTimer(true);
//					if(db2ESImportBuilder.isAsyn()){//强制设置为同步等待模式
//						db2ESImportBuilder.setAsyn(false);
//					}
					dataStream = db2ESImportBuilder.builder();
				}
			}
			catch (DataImportException e){
				if(logger.isErrorEnabled())
					logger.error("ExternalScheduler execute failed:",e);
				throw e;
			}
			catch (Exception e){
				if(logger.isErrorEnabled())
					logger.error("ExternalScheduler execute failed:",e);
				throw new DataImportException("ExternalScheduler execute failed:",e);
			}
			catch (Throwable e){
				if(logger.isErrorEnabled())
					logger.error("ExternalScheduler execute failed:",e);
				throw new DataImportException("ExternalScheduler execute failed:",e);
			}
			finally {
//				lock.unlock();
			}
			if(dataStream == null)
			{
				throw new DataImportException("ExternalScheduler failed: datastream build failed");
			}


//			dataStream.init();

		}
		dataStream.execute();
	}

	public void destroy(){
		if(this.dataStream != null){
			this.dataStream.destroy();
			dataStream = null;
		}
		dataStreamBuilder = null;

	}
}
