package org.frameworkset.tran.metrics.entity;
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
 * <p>Description: 证书范围对象，</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/20 11:20
 * @author biaoping.yin
 * @version 1.0
 */
public class IntegerStage extends Stage{
	private Integer start;
	private Integer end;
	/**
	 * 判断时间匹配本数据段范围
	 * @param num
	 * @return
	 */
	public boolean match(int num){
		if(start != null && end != null){
			if (num >= start && num < end) {
				return true;
			}
		}
		else if(end == null){
			if(num >= start)
				return true;
		}
		return false;
	}

	public Integer getStart() {
		return start;
	}

	public void setStart(Integer start) {
		this.start = start;
	}

	public Integer getEnd() {
		return end;
	}

	public void setEnd(Integer end) {
		this.end = end;
	}
}
