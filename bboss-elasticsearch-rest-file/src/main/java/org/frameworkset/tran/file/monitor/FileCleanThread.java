package org.frameworkset.tran.file.monitor;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/30 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class FileCleanThread extends Thread{

	/**
	 * 备份文件目录
	 */
	private String fileDir;

	/**
	 * 备份文件清理线程执行时间间隔，单位：毫秒
	 * 默认每隔10秒执行一次
	 */
	private long checkInterval = 10000l;
	/**
	 * 文件保留时长，单位：毫秒，超过时长的文件将会被删除
	 * 默认保留7天
	 */
	private long fileLiveTime = 7 * 24 * 60 * 60 * 1000l;
	private static final Logger logger = LoggerFactory.getLogger(FileCleanThread.class);

	public FileCleanThread(String threadName,String fileDir,
						   long checkInterval,
						   long fileLiveTime){
		super(threadName);
		this.fileDir = fileDir;
		this.checkInterval = checkInterval;
		this.fileLiveTime = fileLiveTime;
	}
	public void start(){
		this.setDaemon(true);
		super.start();
	}
	private long getCreateTime(File file ){
		try {
			Path path =  file.toPath();
			BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
			// 创建时间
//			Instant instant = attr.creationTime().toInstant();
			return attr.creationTime().toMillis();
		} catch (IOException e) {
			logger.error("Get createtime of file "+ file.getAbsolutePath() +" failed :",e);
			return -1;
		}

	}
	public void run(){
		File transferSuccessFileDir_ = new File(fileDir);
		logger.info("Start "+ this.getName() +" ,fileDir["+ fileDir +"],fileLiveTime["+ fileLiveTime +"]毫秒");
		while(true){
			long lastArchtime = System.currentTimeMillis() - fileLiveTime ;

			if(transferSuccessFileDir_.exists()){
				File[] files = transferSuccessFileDir_.listFiles();
				for(int i =0 ; i < files.length; i ++){
					File file = files[i];
					if(!file.isFile())
						continue;
					try {
						long filetime = getCreateTime( file );
						if(filetime < 0){
							continue;
						}
						if(filetime <= lastArchtime) {
							file.delete();
							if (logger.isInfoEnabled())
								logger.info("Delete file {} lived {} ms complete.", file.getPath(), fileLiveTime);
						}
					}
					catch (Exception e){
						logger.error("Delete file "+ file.getPath() + " failed:",e);
					}
					catch (Throwable e){
						logger.error("Delete file "+ file.getPath() + " failed:",e);
					}
				}
			}

			try {
				synchronized (this) {
					wait(checkInterval);
				}
			} catch (InterruptedException e) {
				logger.error("Delete file thread Interrupted",e);
				break;
			}
		}
	}
}
