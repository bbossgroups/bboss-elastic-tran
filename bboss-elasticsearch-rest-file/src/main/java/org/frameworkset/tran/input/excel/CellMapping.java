package org.frameworkset.tran.input.excel;
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
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/2/7 17:32
 * @author biaoping.yin
 * @version 1.0
 */
public class CellMapping {
	public static final int CELL_BOOLEAN = 5;
	public static final int CELL_DATE = 3;

	public static final int CELL_NUMBER = 2;
	public static final int CELL_STRING = 1;

	private int cell;
	private String fieldName;
	private int cellType = CELL_STRING;
	private String dateFormat;
	private String numberFormat;
	private Object defaultValue;

	public int getCell() {
		return cell;
	}

	public void setCell(int cell) {
		this.cell = cell;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public int getCellType() {
		return cellType;
	}

	public void setCellType(int cellType) {
		this.cellType = cellType;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getNumberFormat() {
		return numberFormat;
	}

	public void setNumberFormat(String numberFormat) {
		this.numberFormat = numberFormat;
	}

	public Object getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}
	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append("{");
		/**
		 * cell;
		 * 	private String fieldName;
		 * 	private int cellType = CELL_STRING;
		 * 	private String dateFormat;
		 * 	private String numberFormat;
		 * 	private Object defaultValue;
		 */
		builder.append("fieldName:").append(fieldName);
		builder.append(",cell:").append(cell);
		builder.append(",cellType:").append(convertType(cellType));
		builder.append(",dateFormat:").append(dateFormat);
		builder.append(",numberFormat:").append(numberFormat);
		builder.append(",defaultValue:").append(defaultValue);
		builder.append("}");
		return builder.toString();

	}
	private String convertType(int cellType){
		if(cellType == CELL_BOOLEAN) return "CELL_BOOLEAN";
		else if(cellType == CELL_DATE) return "CELL_DATE";
		else if(cellType == CELL_NUMBER) return "CELL_NUMBER";
		else if(cellType == CELL_STRING) return "CELL_STRING";
		else return "CELL_UNKOWN";
	}
}
