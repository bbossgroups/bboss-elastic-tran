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

import com.frameworkset.util.SimpleStringUtil;
import com.frameworkset.util.ValueObjectUtil;

import java.math.BigDecimal;
import java.util.Date;
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
public class FieldMappingUtil {
	public static void buildRecord(Map result, String line, List<CellMapping> cellMappingList,String splitChar){
		String[] vs = line.split(splitChar);
		if(cellMappingList != null) {
			for (CellMapping cellMapping : cellMappingList) {
				if(cellMapping.getCell() >= vs.length)
					continue;
				String value = vs[cellMapping.getCell()];
				result.put(cellMapping.getFieldName(), convertValue(cellMapping, value));
			}
		}
	}

	private static Object convertValue(CellMapping cellMapping,String value ){
		if(cellMapping.getCellType() == CellMapping.CELL_STRING){
			return value;
		}

		else if(cellMapping.getCellType() == CellMapping.CELL_NUMBER_INTEGER){
			return ValueObjectUtil.typeCast(value,Integer.class);
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_NUMBER_LONG){
			return ValueObjectUtil.typeCast(value,Long.class);
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_NUMBER_FLOAT){
			return ValueObjectUtil.typeCast(value,Float.class);
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_NUMBER_DOUBLE){
			return ValueObjectUtil.typeCast(value,Double.class);
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_DATE){
			if(SimpleStringUtil.isEmpty(cellMapping.getDateFormat())) {
				return ValueObjectUtil.typeCast(value, Date.class);
			}
			else{
				return ValueObjectUtil.typeCast(value,value.getClass(), Date.class,cellMapping.getDateFormat());
			}
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_NUMBER){
			return ValueObjectUtil.typeCast(value, BigDecimal.class);
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_BOOLEAN){
			return ValueObjectUtil.typeCast(value, Boolean.class);
		}
		else if(cellMapping.getCellType() == CellMapping.CELL_NUMBER_SHORT){
			return ValueObjectUtil.typeCast(value, Short.class);
		}
		else{
			return value;
		}

	}

}
