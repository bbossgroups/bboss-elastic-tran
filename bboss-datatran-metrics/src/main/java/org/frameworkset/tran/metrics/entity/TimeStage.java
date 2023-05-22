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
 * <p>Description: 时间范围对象，时间单位：毫秒</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/20 11:20
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeStage extends  Stage{


	private Long start;
	private Long end;



	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}

	public Long getEnd() {
		return end;
	}

	public void setEnd(Long end) {
		this.end = end;
	}


	/**
	 * 判断时间匹配本时间段范围
	 * @param time
	 * @return
	 */
	public boolean match(long time){
		if(start != null && end != null){
			if(start == 0l) {
				if (time >= start && time <= end) {
					return true;
				}
			}
			else{
				if (time > start && time <= end) {
					return true;
				}
			}
		}
		else if(start != null){
			if(time > start)
				return true;
		}
		return false;
	}



}
