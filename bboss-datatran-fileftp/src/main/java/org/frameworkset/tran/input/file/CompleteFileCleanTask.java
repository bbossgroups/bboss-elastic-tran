package org.frameworkset.tran.input.file;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.util.StoppedThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/11/16
 * @author biaoping.yin
 * @version 1.0
 */
public class CompleteFileCleanTask extends StoppedThread {
	private static final Logger logger = LoggerFactory.getLogger(CompleteFileCleanTask.class);
	private BlockingQueue<File> completeCleanFiles = new ArrayBlockingQueue<File>(1000) ;
	public CompleteFileCleanTask(ImportContext importContext){
		super("CompleteFileCleanTask-jobName["+importContext.getJobName()+"]");

	}
	@Override
	public void start(){
		this.setDaemon(true);
		super.start();
	}
	public void addFile(File file)  {

		try {
			completeCleanFiles.put(file);
		} catch (InterruptedException e) {
			logger.warn("",e);
		}
	}

	@Override
	public void run(){
		while(true){
			try {
				File file = completeCleanFiles.take();
				try {
					logger.info("Delete complete file {}",file.getAbsolutePath());
					file.delete();
				}
				catch (Exception e){
					logger.error(file.getAbsolutePath(),e);
				}
			} catch (InterruptedException e) {
				if(logger.isDebugEnabled())
					logger.debug("",e);
				break;
			} catch (Exception e) {
				logger.warn("completeCleanFiles failed:",e);
			}
		}
	}
}
