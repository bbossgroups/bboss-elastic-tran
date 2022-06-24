package org.frameworkset.tran.input.file;
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

/**
 * <p>Description:  如果指定了closeOlderTime，但是有些文件是特例不能不关闭，那么可以通过指定CloseOldedFileAssert来
 * 	  检查静默时间达到closeOlderTime的文件是否需要被关闭</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/28 9:53
 * @author biaoping.yin
 * @version 1.0
 */
public interface CloseOldedFileAssert {
	/**
	 * 如果指定了closeOlderTime，但是有些文件是特例不能不关闭，那么可以通过指定CloseOldedFileAssert来
	 * 检查静默时间达到closeOlderTime的文件是否需要被关闭
	 * true 关闭
	 * false 不能关闭
	 * @param fileInfo
	 * @return
	 */
	public boolean canClose(FileInfo fileInfo);
}
