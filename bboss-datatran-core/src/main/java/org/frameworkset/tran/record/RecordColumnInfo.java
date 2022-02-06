package org.frameworkset.tran.record;
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

import java.text.DateFormat;

/**
 * <p>Description: 保存记录字段信息</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/24 16:19
 * @author biaoping.yin
 * @version 1.0
 */
public class RecordColumnInfo {
	private boolean dateTag;
	private DateFormat dateFormat;
	public boolean isDateTag() {
		return dateTag;
	}

	public void setDateTag(boolean dateTag) {
		this.dateTag = dateTag;
	}

	public DateFormat getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(DateFormat dateFormat) {
		this.dateFormat = dateFormat;
	}
}
