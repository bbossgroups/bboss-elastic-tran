package org.frameworkset.tran.es.output;
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

import org.frameworkset.tran.config.BaseImportConfig;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/26 14:54
 * @author biaoping.yin
 * @version 1.0
 */
public class ESOutputConfig  extends BaseImportConfig {
	private String targetIndex;
	private String targetIndexType;

	public String getTargetIndex() {
		return targetIndex;
	}

	public ESOutputConfig setTargetIndex(String targetIndex) {
		this.targetIndex = targetIndex;
		return this;
	}

	public String getTargetIndexType() {
		return targetIndexType;
	}

	public ESOutputConfig setTargetIndexType(String targetIndexType) {
		this.targetIndexType = targetIndexType;
		return this;
	}

}
