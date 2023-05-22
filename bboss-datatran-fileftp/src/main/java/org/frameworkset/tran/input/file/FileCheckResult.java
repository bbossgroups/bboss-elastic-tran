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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/11/16
 * @author biaoping.yin
 * @version 1.0
 */
public class FileCheckResult {
	public static final int FileCheckResult_NewFile = 1;
	public static final int FileCheckResult_CollectingFile = 2;
	public static final int FileCheckResult_CompleteFile = 3;
	public static final int FileCheckResult_OldFile = 5;
	public static final int FileCheckResult_LostedFile = 7;
	public static final int FileCheckResult_FailedFile = 8;

	public static final int FileCheckResult_StopTask = 6;
	private int result;
	private FileReaderTask fileReaderTask;

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public FileReaderTask getFileReaderTask() {
		return fileReaderTask;
	}

	public void setFileReaderTask(FileReaderTask fileReaderTask) {
		this.fileReaderTask = fileReaderTask;
	}
}
