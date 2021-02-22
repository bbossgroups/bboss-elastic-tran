package org.frameworkset.tran.output.fileftp;
/**
 * Copyright 2020 bboss
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

import com.frameworkset.util.SimpleStringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * <p>Description: 失败文件重传</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/4 14:48
 * @author biaoping.yin
 * @version 1.0
 */
public class SuccessFilesClean extends Thread{
	private String transferSuccessFileDir;
	private static final Logger logger = LoggerFactory.getLogger(SuccessFilesClean.class);
	private FileFtpOupputContext fileFtpOupputContext;
	public SuccessFilesClean(FileFtpOupputContext fileFtpOupputContext){
		super("SuccessFilesClean-Thread");
		this.fileFtpOupputContext = fileFtpOupputContext;
		transferSuccessFileDir = SimpleStringUtil.getPath(fileFtpOupputContext.getFileDir(),"transferSuccessFileDir");

	}
	public void start(){
		this.setDaemon(true);
		super.start();
	}
	public void run(){
		File transferSuccessFileDir_ = new File(transferSuccessFileDir);
		logger.info("SuccessFilesClean-Thread started,transferSuccessFileDir["+transferSuccessFileDir+"],FileLiveTime["+fileFtpOupputContext.getFileLiveTime() +"]秒");
		while(true){
			long lastArchtime = System.currentTimeMillis() - fileFtpOupputContext.getFileLiveTime() * 1000L;

			if(transferSuccessFileDir_.exists()){
				File[] files = transferSuccessFileDir_.listFiles();
				for(int i =0 ; i < files.length; i ++){
					File file = files[i];
					if(!file.isFile())
						continue;
					try {
						long filetime = file.lastModified();
						if(filetime <= lastArchtime) {
							file.delete();
							if (logger.isInfoEnabled())
								logger.info("Delete success file " + file.getPath() + " complete.");
						}
					}
					catch (Exception e){
						logger.error("Delete success file "+ file.getPath() + " failed:",e);
					}
					catch (Throwable e){
						logger.error("Delete success file "+ file.getPath() + " failed:",e);
					}
				}
			}

			try {
				synchronized (this) {
					wait(fileFtpOupputContext.getSuccessFilesCleanInterval());
				}
			} catch (InterruptedException e) {
				logger.error("Delete success file thread Interrupted",e);
				break;
			}
		}
	}



}
