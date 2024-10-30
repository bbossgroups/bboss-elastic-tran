package org.frameworkset.tran.plugin.hbase.output;
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

import com.frameworkset.util.SimpleStringUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.nosql.hbase.HBaseAccessException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.hbase.HBasePluginConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseOutputConfig extends HBasePluginConfig implements OutputConfig {

	private Map<String, List<FamilyColumnMapping>> familyColumnMappings;
	private String rowKeyField;
	private String globalFamiliy;
	private byte[] bglobalFamiliy = Bytes.toBytes("df");
	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		if(SimpleStringUtil.isEmpty(rowKeyField)){
			throw new HBaseAccessException("rowKeyField not setted , please use HBaseOutputConfig.setRowKeyField(String rowKeyField) to set rowKeyField.");
		}
		if(SimpleStringUtil.isNotEmpty(globalFamiliy))
			bglobalFamiliy = Bytes.toBytes(globalFamiliy);

	}

	public byte[] getBglobalFamiliy() {
		return bglobalFamiliy;
	}

	public String getGlobalFamiliy() {
		return globalFamiliy;
	}

	/**
	 * 指定全局列簇名称，默认为df，如果指定了自定义的列簇映射关系，全局列簇不起作用
	 * @param globalFamiliy
	 * @return
	 */
	public HBaseOutputConfig setFamiliy(String globalFamiliy){
		this.globalFamiliy = globalFamiliy;
		return this;
	}

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new HBaseOutputDataTranPlugin(importContext);
	}

	/**
	 * 指定自定义列簇与源字段映射关系，只有映射过的源字段值才会保存到hbase表
	 * 如果没有指定自定义列簇映射关系将采用全局列簇名称
	 * @param family hbase列簇名称
	 * @param field 源字段名称，同时也是列的名称
	 * @return
	 */
	public HBaseOutputConfig addFamilyColumnMapping(String family,String field){
		return addFamilyColumnMapping( family, field,field);
	}

	/**
	 * 指定自定义列簇与源字段映射关系，只有映射过的源字段值才会保存到hbase表
	 * 如果没有指定自定义列簇映射关系将采用全局列簇名称
	 * @param family hbase列簇名称
	 * @param field  源字段名称
	 * @param column  hbase列名称 ，field对应的值作为列的值
	 * @return
	 */
	public HBaseOutputConfig addFamilyColumnMapping(String family,String field,String column){
		if(familyColumnMappings == null){
			familyColumnMappings = new LinkedHashMap<>();
		}
		FamilyColumnMapping familyColumnMapping = new FamilyColumnMapping();
		familyColumnMapping.setColumn(column);
		familyColumnMapping.setFamily(family);
		familyColumnMapping.init();
		List<FamilyColumnMapping> familyColumnMappings_ =  familyColumnMappings.get(field);
		if(familyColumnMappings_ == null){
			familyColumnMappings_ = new ArrayList<>();
			familyColumnMappings.put(field,familyColumnMappings_);
		}
		familyColumnMappings_.add(familyColumnMapping);
		return this;

	}

	/**
	 * 指定rowkey对应的源字段名称，必须指定
	 * @param rowKeyField
	 * @return
	 */
	public HBaseOutputConfig setRowKeyField(String rowKeyField){
		this.rowKeyField = rowKeyField;
		return this;
	}

	public String getRowKeyField() {
		return rowKeyField;
	}

	public Map<String, List<FamilyColumnMapping>> getFamilyColumnMappings() {
		return familyColumnMappings;
	}

	public List<FamilyColumnMapping> getFamilyColumnMappings(String field) {
		return familyColumnMappings.get(field);
	}


}
