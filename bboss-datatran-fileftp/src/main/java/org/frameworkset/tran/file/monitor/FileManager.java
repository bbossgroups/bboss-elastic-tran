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

import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/30 15:52
 * @author biaoping.yin
 * @version 1.0
 */
public class FileManager {
	private static final Logger logger = LoggerFactory.getLogger(FileManager.class);

	/**
	 * {creationTime,lastModifiedTime,lastAccessTime};
	 * @param file
	 * @return
	 */
	public static  long[] getFileTimestamps(File file ){
		try {
			Path path =  file.toPath();
			BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
			// 创建时间
//			Instant instant = attr.creationTime().toInstant();
			long[] times = new long[]
					{attr.creationTime().toMillis(),attr.lastModifiedTime().toMillis(),attr.lastAccessTime().toMillis()};
			return times;
		} catch (IOException e) {

			logger.error("Get createtime of file "+ file.getAbsolutePath() +" failed :",e);
			return null;
		}

	}

	/**
	 * {creationTime,lastModifiedTime,lastAccessTime};
	 * @param file
	 * @return
	 */
	public static  Date[] getFileDates(File file ){
		try {
			Path path =  file.toPath();
			BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
			// 创建时间
//			Instant instant = attr.creationTime().toInstant();
			Date[] times = new Date[]
					{new Date(attr.creationTime().toMillis()),new Date(attr.lastModifiedTime().toMillis()),new Date(attr.lastAccessTime().toMillis())};
			return times;
		} catch (IOException e) {

			logger.error("Get createtime of file "+ file.getAbsolutePath() +" failed :",e);
			return null;
		}

	}

	public static  long getATime(RemoteResourceInfo file ){

		FileAttributes attr = file.getAttributes();

		// 创建时间
//			Instant instant = attr.creationTime().toInstant();
		return attr.getAtime();


	}

	public static long getFileLastTimestamp(File file){
		//{creationTime,lastModifiedTime,lastAccessTime};
		long[] fileTimestamps = FileManager.getFileTimestamps(file);
		if(fileTimestamps == null)
			return -1;

		if( fileTimestamps[1] >= fileTimestamps[0]){
			return fileTimestamps[1];
		}
		else{
			return fileTimestamps[0];
		}
	}
}
