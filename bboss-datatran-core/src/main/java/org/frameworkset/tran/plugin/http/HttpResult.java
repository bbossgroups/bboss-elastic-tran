package org.frameworkset.tran.plugin.http;
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

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.frameworkset.tran.plugin.http.input.QueryAction;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/1
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpResult<T> {
	/**
	 * 从报文中提取的待处理数据集合
	 */
	private List<T> datas;
	private ClassicHttpResponse response;
    private int seq;
    private QueryAction queryAction;
	/**
	 * 解析报文得到的对象
	 */
	private Object parseredObject;

	public List<T> getDatas() {
		return datas;
	}

	public int size(){
		return datas != null? datas.size():0;
	}

	public void setDatas(List<T> datas) {
		this.datas = datas;
	}

	public ClassicHttpResponse getResponse() {
		return response;
	}

	public void setResponse(ClassicHttpResponse response) {
		this.response = response;
	}

	public Object getParseredObject() {
		return parseredObject;
	}

	public void setParseredObject(Object parseredObject) {
		this.parseredObject = parseredObject;
	}

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public QueryAction getQueryAction() {
        return queryAction;
    }

    public void setQueryAction(QueryAction queryAction) {
        this.queryAction = queryAction;
    }

    public boolean hasMore() {
        return queryAction.hasMore();
    }
}
