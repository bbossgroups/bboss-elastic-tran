package org.frameworkset.tran.ftp;
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

import org.apache.commons.net.ftp.FTPFile;
import org.frameworkset.tran.input.file.FilterFileInfo;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/1/4 22:12
 * @author biaoping.yin
 * @version 1.0
 */
public class FTPFilterFileInfo implements FilterFileInfo {
	private FTPFile file;
	private String remoteParent;
	private String name ;
	public FTPFilterFileInfo(String remoteParent,FTPFile file){
		this.file = file;
		this.remoteParent = remoteParent;
		name = file.getName().trim();
	}
	@Override
	public String getParentDir() {
		return remoteParent;
	}

	@Override
	public String getFileName() {
		return name;
	}

	@Override
	public boolean isDirectory() {
		return file.isDirectory();
	}

	@Override
	public Object getFileObject() {
		return file;
	}
}
