package org.frameworkset.tran.record;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/12/8
 * @author biaoping.yin
 * @version 1.0
 */
public class FieldMappingManager {
	/**
	 * 文本字段切割符
	 */
	private String fieldSplit;
	/**
	 * 通过fieldSplit切割的字段位置、对应字段名称、字段类型、字段默认值、字段格式映射配置
	 */
	private List<CellMapping> cellMappingList;
	private Map<Integer,CellMapping> cellMappings;
	public FieldMappingManager(){

	}
	public List<CellMapping> getCellMappingList() {
		return cellMappingList;
	}


	public FieldMappingManager addCellMapping(int cell, String field){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		return addCellMapping(  cellMapping );
	}
	public FieldMappingManager addCellMapping(int cell,String field,Object defaultValue){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setDefaultValue(defaultValue);
		return addCellMapping(  cellMapping );
	}
	public FieldMappingManager addDateCellMapping(int cell,String field,int cellType,Object defaultValue,String dateformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellType(cellType);
		cellMapping.setDateFormat(dateformat);
		cellMapping.setDefaultValue(defaultValue);
		return addCellMapping(  cellMapping );
	}
	public FieldMappingManager addNumberCellMapping(int cell,String field,int cellType,Object defaultValue,String numberformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellType(cellType);
		cellMapping.setNumberFormat(numberformat);
		cellMapping.setDefaultValue(defaultValue);
		return addCellMapping(  cellMapping );
	}

	public FieldMappingManager addDateCellMapping(int cell,String field,int cellType,String dateformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellType(cellType);
		cellMapping.setDateFormat(dateformat);
		return addCellMapping(  cellMapping );
	}
	public FieldMappingManager addNumberCellMapping(int cell,String field,int cellType,String numberformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellType(cellType);
		cellMapping.setNumberFormat(numberformat);
		return addCellMapping(  cellMapping );
	}
	public FieldMappingManager addCellMappingWithType(int cell,String field,int cellType ){

		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellType(cellType);
		return addCellMapping(  cellMapping );
	}

	public FieldMappingManager addCellMappingWithType(int cell,String field,int cellType ,Object defaultValue){

		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellType(cellType);
		cellMapping.setDefaultValue(defaultValue);
		return addCellMapping(  cellMapping );
	}
	public FieldMappingManager addCellMapping(CellMapping cellMapping ){
		if(cellMappingList == null) {
			cellMappingList = new ArrayList<>();
			cellMappings = new LinkedHashMap<>();
		}
		cellMappingList.add(cellMapping);
		cellMappings.put(cellMapping.getCell(),cellMapping);
		return this;
	}
	protected void appendFieldList(StringBuilder stringBuilder){
		for(int i =0; this.cellMappingList != null &&  i < this.cellMappingList.size(); i ++){
			stringBuilder.append(",").append(this.cellMappingList.get(i));
		}
	}

	public String getFieldSplit() {
		return fieldSplit;
	}

	public void setFieldSplit(String fieldSplit) {
		this.fieldSplit = fieldSplit;
	}
}
