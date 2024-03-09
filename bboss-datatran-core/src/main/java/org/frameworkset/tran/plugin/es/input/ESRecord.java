package org.frameworkset.tran.plugin.es.input;
/**
 * Copyright 2008 biaoping.yin
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


import org.frameworkset.elasticsearch.entity.MetaMap;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.BaseRecord;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/19 11:24
 * @author biaoping.yin
 * @version 1.0
 */
public class ESRecord  extends BaseRecord {
	private MetaMap data;
	public ESRecord(TaskContext taskContext, ImportContext importContext, Object data){
		super(taskContext,  importContext);
		this.data = (MetaMap) data;
        initMetaDatas();
	}
    private void initMetaDatas(){
        Map<String,Object> tmp = new LinkedHashMap<>();
        tmp.put("_id", data.getId());
        tmp.put("type", data.getType());

        /**文档对应索引字段信息*/
//		private Map<String, List<Object>> fields;
        tmp.put("fields", data.getFields());
/**文档对应版本信息*/
//		private long version;
        tmp.put("version", data.getVersion());
        /**文档对应的索引名称*/
//		private String index;
        tmp.put("index", data.getIndex());
        /**文档对应的高亮检索信息*/
//		private Map<String,List<Object>> highlight;
        tmp.put("highlight", data.getHighlight());
        /**文档对应的排序信息*/
//		private Object[] sort;
        tmp.put("sort", data.getSort());
        /**文档对应的评分信息*/
//		private Double  score;
        tmp.put("score", data.getScore());
        /**文档对应的父id*/
//		private Object parent;
        tmp.put("parent", data.getParent());
        /**文档对应的路由信息*/
//		private Object routing;
        tmp.put("routing", data.getRouting());
        /**文档对应的是否命中信息*/
//		private boolean found;
        tmp.put("found", data.isFound());
        /**文档对应的nested检索信息*/
//		private Map<String,Object> nested;
        tmp.put("nested", data.getNested());
        /**文档对应的innerhits信息*/
        tmp.put("innerHits", data.getInnerHits());
//		private Map<String,Map<String, InnerSearchHits>> innerHits;
        /**文档对应的索引分片号*/
//		private String shard;
        tmp.put("shard", data.getShard());
        /**文档对应的elasticsearch集群节点名称*/
//		private String node;
        tmp.put("node", data.getNode());
        /**文档对应的打分规则信息*/
//		private Explanation explanation;
        tmp.put("explanation", data.getExplanation());

//		private long seqNo;//"_index": "trace-2017.09.01",
        tmp.put("seqNo", data.getSeqNo());
//		private long primaryTerm;//"_index": "trace-2017.09.01",
        tmp.put("primaryTerm", data.getPrimaryTerm());
        this.setMetaDatas(tmp);
    }

	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

//	@Override
//	public Date getDateTimeValue(String colName) throws ESDataImportException {
//		Object value = getValue(  colName);
//		if(value == null)
//			return null;
//		return TranUtil.getDateTimeValue(colName,value,taskContext.getImportContext());
//	}

	@Override
	public Object getValue(String colName) {
		return data.get(colName);
	}
//
//	public Object getMetaValue(String colName) {
//		/**文档_id*/
////		private String id;
//		if(colName.equals("_id"))
//			return data.getId();
//		/**文档对应索引类型信息*/
////		private String  type;
//		if(colName.equals("type"))
//			return data.getType();
//		/**文档对应索引字段信息*/
////		private Map<String, List<Object>> fields;
//		if(colName.equals("fields"))
//			return data.getFields();
///**文档对应版本信息*/
////		private long version;
//		if(colName.equals("version"))
//			return data.getVersion();
//		/**文档对应的索引名称*/
////		private String index;
//		if(colName.equals("index"))
//			return data.getIndex();
//		/**文档对应的高亮检索信息*/
////		private Map<String,List<Object>> highlight;
//		if(colName.equals("highlight"))
//			return data.getHighlight();
//		/**文档对应的排序信息*/
////		private Object[] sort;
//		if(colName.equals("sort"))
//			return data.getSort();
//		/**文档对应的评分信息*/
////		private Double  score;
//		if(colName.equals("score"))
//			return data.getScore();
//		/**文档对应的父id*/
////		private Object parent;
//		if(colName.equals("parent"))
//			return data.getParent();
//		/**文档对应的路由信息*/
////		private Object routing;
//		if(colName.equals("routing"))
//			return data.getRouting();
//		/**文档对应的是否命中信息*/
////		private boolean found;
//		if(colName.equals("found"))
//			return data.isFound();
//		/**文档对应的nested检索信息*/
////		private Map<String,Object> nested;
//		if(colName.equals("nested"))
//			return data.getNested();
//		/**文档对应的innerhits信息*/
////		private Map<String,Map<String, InnerSearchHits>> innerHits;
//		if(colName.equals("innerHits"))
//			return data.getInnerHits();
//		/**文档对应的索引分片号*/
////		private String shard;
//		if(colName.equals("shard"))
//			return data.getShard();
//		/**文档对应的elasticsearch集群节点名称*/
////		private String node;
//		if(colName.equals("node"))
//			return data.getNode();
//		/**文档对应的打分规则信息*/
////		private Explanation explanation;
//		if(colName.equals("explanation"))
//			return data.getExplanation();
//
////		private long seqNo;//"_index": "trace-2017.09.01",
//		if(colName.equals("seqNo"))
//			return data.getSeqNo();
////		private long primaryTerm;//"_index": "trace-2017.09.01",
//		if(colName.equals("primaryTerm"))
//			return data.getPrimaryTerm();
//		throw new DataImportException("Get Meta Value failed: " + colName + " is not a elasticsearch document meta field.");
//	}

	@Override
	public long getOffset() {
		return 0;
	}

	@Override
	public Object getKeys() {
        if(data == null)
            return null;
		return data.keySet();
	}
	public Object getData(){
		return data;
	}

}
