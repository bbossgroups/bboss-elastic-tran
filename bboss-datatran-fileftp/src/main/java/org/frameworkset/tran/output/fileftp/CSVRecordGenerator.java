package org.frameworkset.tran.output.fileftp;
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

import com.frameworkset.util.SimpleStringUtil;
import com.opencsv.CSVWriter;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.file.output.CSVFileOutputConfig;
import org.frameworkset.tran.record.CellMapping;
import org.frameworkset.tran.util.HeaderRecordGeneratorV1;
import org.frameworkset.tran.util.RecordGeneratorContext;
import org.frameworkset.tran.util.RecordGeneratorV1;
import org.frameworkset.util.DataFormatUtil;

import java.io.Writer;
import java.util.Date;
import java.util.List;

import static org.frameworkset.tran.record.CellMapping.DEFAULT_DATEFORMATTER;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:52
 * @author biaoping.yin
 * @version 1.0
 */
public class CSVRecordGenerator  {
//	public void buildRecord(TaskContext context, TaskMetrics taskMetrics, CommonRecord record, Writer builder){
    public static void buildRecord(CSVFileOutputConfig outputConfig,CommonRecord record,CSVWriter writer){
        List<CellMapping> cellMappingList = outputConfig.getCellMappingList();
        String[] datas = new String[cellMappingList.size()];
        CellMapping cellMapping = null;
        for(int i = 0; i < cellMappingList.size(); i ++){
            cellMapping = cellMappingList.get(i);
            Object value = record.getData(cellMapping.getFieldName());
            datas[i] = getCellValue(cellMapping,value);
        }
        writer.writeNext( datas);
	 
	}

    private static String getCellValue(CellMapping cellMapping, Object value) {
        if(value instanceof String){
            return (String)value;
        }
        else if(value instanceof Date){
            String dateFormatter = cellMapping.getDateFormat();
            if(SimpleStringUtil.isEmpty(dateFormatter)){
                dateFormatter = DEFAULT_DATEFORMATTER;
            }
            String _value = DataFormatUtil.formatDate((Date)value,dateFormatter);
            return _value;
        }
        else { 
            return String.valueOf( value); 
        }
      
        
    }

    /**
     * 构建头行数据方法
     *
     * @throws Exception
     */
    public static void buildHeaderRecord(CSVFileOutputConfig outputConfig, CSVWriter writer) throws Exception {

        List<CellMapping> cellMappingList = outputConfig.getCellMappingList();
        String[] titles = new String[cellMappingList.size()];
        CellMapping cellMapping = null;
        for(int i = 0; i < cellMappingList.size(); i ++){
            cellMapping = cellMappingList.get(i);
            titles[i] = getCellTitle(cellMapping);
        }
        writer.writeNext( titles);
       
    }
    private static String getCellTitle(CellMapping cellMapping){
        String cellTitle = cellMapping.getCellTitle();
        if(cellTitle == null){
            cellTitle = cellMapping.getFieldName();
        }
        return cellTitle;
    }
}
