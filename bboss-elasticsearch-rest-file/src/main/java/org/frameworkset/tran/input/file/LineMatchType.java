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
 * <p>Description: 文件记录包含与排除条件匹配类型</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/23 14:49
 * @author biaoping.yin
 * @version 1.0
 */
public enum  LineMatchType {
	REGEX_MATCH("REGEX_MATCH"),REGEX_CONTAIN("REGEX_CONTAIN"),STRING_CONTAIN("STRING_CONTAIN"),
	STRING_EQUALS("STRING_EQUALS"),STRING_PREFIX("STRING_PREFIX"),STRING_END("STRING_END");
	private String code;
	LineMatchType(String code){
		this.code = code;
	}

	public String getCode() {
		return code;
	}
}
