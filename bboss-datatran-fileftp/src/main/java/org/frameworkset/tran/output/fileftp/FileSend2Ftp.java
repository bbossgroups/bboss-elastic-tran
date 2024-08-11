package org.frameworkset.tran.output.fileftp;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.input.file.RemoteFileChannel;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: 文件异步发送处理器</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/2/7 14:48
 * @author biaoping.yin
 * @version 1.0
 */
public class FileSend2Ftp {
	private RemoteFileChannel remoteFileChannel = null;
	private static final Logger logger = LoggerFactory.getLogger(FileSend2Ftp.class);
	private FileOutputConfig fileOutputConfig;
	public FileSend2Ftp(FileOutputConfig fileOutputConfig){
		this.fileOutputConfig = fileOutputConfig;

	}
	public void init(){
		if(remoteFileChannel == null) {
			remoteFileChannel = new RemoteFileChannel();
			//用远程文件路径作为线程池名称
			remoteFileChannel.setThreadName(fileOutputConfig.getFileSendThreadName());
			remoteFileChannel.setWorkThreads(fileOutputConfig.getSendFileAsynWorkThreads());
			remoteFileChannel.init();
		}
	}

	public RemoteFileChannel getRemoteFileChannel() {
		return remoteFileChannel;
	}

	public void destroy(){
		if(remoteFileChannel != null){
			remoteFileChannel.destroy();
		}
	}

	public void submitNewTask(Runnable runnable){

		if(remoteFileChannel != null){
			remoteFileChannel.submitNewTask(runnable);
		}

	}
}
