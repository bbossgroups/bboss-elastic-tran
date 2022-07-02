package org.frameworkset.tran.plugin.db.input;
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

import com.frameworkset.common.poolman.Param;
import org.frameworkset.tran.CommonRecord;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/13 22:41
 * @author biaoping.yin
 * @version 1.0
 */
public class DBRecord extends CommonRecord {
	private List<Param> params;
	public static final int INSERT = 0;
	public static final int DELETE = 2;
	public static final int UPDATE = 1;


	private int action = INSERT;
	public boolean isInsert(){
		return action ==  INSERT;
	}

	public boolean isDelete(){
		return action ==  DELETE;
	}

	public boolean isUpate(){
		return action ==  UPDATE;
	}


	public int getAction() {
		return action;
	}

	public void setAction(int action) {
		this.action = action;
	}

	public List<Param> getParams() {
		return params;
	}
	public void setParams(List<Param> params) {
		this.params = params;
	}

	public int size(){
		if(params != null)
			return params.size();
		return 0;
	}
	public Param get(int idx){
		return params.get(idx);
	}

}
