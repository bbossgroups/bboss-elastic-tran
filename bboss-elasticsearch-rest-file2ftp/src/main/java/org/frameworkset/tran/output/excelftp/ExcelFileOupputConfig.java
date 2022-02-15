package org.frameworkset.tran.output.excelftp;
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

import org.frameworkset.tran.input.excel.CellMapping;
import org.frameworkset.tran.output.fileftp.FileOupputConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class ExcelFileOupputConfig extends FileOupputConfig {
	/**
	 * excel 标题行，为空时忽略
	 */
	private String title;
	/**
	 * excel sheet名称
	 */
	private String sheetName;
	/**
	 * 列索引、列名称、列对应的字段field名称映射关系
	 */
	private List<CellMapping> cellMappingList = new ArrayList<>();

	public String getTitle() {
		return title;
	}

	public ExcelFileOupputConfig setTitle(String title) {
		this.title = title;
		return this;
	}

	public String getSheetName() {
		return sheetName;
	}

	public ExcelFileOupputConfig setSheetName(String sheetName) {
		this.sheetName = sheetName;
		return this;
	}

	public List<CellMapping> getCellMappingList() {
		return cellMappingList;
	}



	public ExcelFileOupputConfig addCellMapping(int cell, String field,String cellTitle){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setCellTitle(cellTitle);
		return addCellMapping(  cellMapping );
	}
	public ExcelFileOupputConfig addCellMapping(int cell,String field,String cellTitle,Object defaultValue){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setDefaultValue(defaultValue);
		cellMapping.setCellTitle(cellTitle);
		return addCellMapping(  cellMapping );
	}
	public ExcelFileOupputConfig addDateCellMapping(int cell,String field,String cellTitle,Object defaultValue,String dateformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setDateFormat(dateformat);
		cellMapping.setDefaultValue(defaultValue);
		cellMapping.setCellTitle(cellTitle);
		return addCellMapping(  cellMapping );
	}
	public ExcelFileOupputConfig addNumberCellMapping(int cell,String field,String cellTitle,Object defaultValue,String numberformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setNumberFormat(numberformat);
		cellMapping.setDefaultValue(defaultValue);
		cellMapping.setCellTitle(cellTitle);
		return addCellMapping(  cellMapping );
	}

	public ExcelFileOupputConfig addDateCellMapping(int cell,String field,String cellTitle,String dateformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setDateFormat(dateformat);
		cellMapping.setCellTitle(cellTitle);
		return addCellMapping(  cellMapping );
	}
	public ExcelFileOupputConfig addNumberCellMapping(int cell,String field,String cellTitle,String numberformat){
		CellMapping cellMapping = new CellMapping();
		cellMapping.setCell(cell);
		cellMapping.setFieldName(field);
		cellMapping.setNumberFormat(numberformat);
		cellMapping.setCellTitle(cellTitle);
		return addCellMapping(  cellMapping );
	}
	public ExcelFileOupputConfig addCellMapping(CellMapping cellMapping ){

		cellMappingList.add(cellMapping);
		return this;
	}

}
