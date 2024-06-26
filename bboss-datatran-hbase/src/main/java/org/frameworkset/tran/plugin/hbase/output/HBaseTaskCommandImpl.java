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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.nosql.hbase.HBaseHelper;
import org.frameworkset.nosql.hbase.HBaseHelperFactory;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: import datas to database task command</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseTaskCommandImpl extends BaseTaskCommand<  String> {
	private HBaseOutputConfig hBaseOutputConfig;
	private String taskInfo;
	private static final Logger logger = LoggerFactory.getLogger(HBaseTaskCommandImpl.class);
	public HBaseTaskCommandImpl(TaskCommandContext taskCommandContext) {
		super(  taskCommandContext);
//		this.records = datas;
		hBaseOutputConfig = (HBaseOutputConfig) importContext.getOutputConfig();
		this.taskInfo = taskCommandContext.getTaskInfo();

	}

 
 
	private int tryCount;



	 

	private byte[] toBytes(Object data){
		if(data == null)
			return null;
		if(data instanceof String) {
			return Bytes.toBytes((String) data);
		}
		else if(data instanceof Long) {
			return Bytes.toBytes((Long) data);
		}
		else if(data instanceof Double) {
			return Bytes.toBytes((Double) data);
		}
		else if(data instanceof Float) {
			return Bytes.toBytes((Float) data);
		}
		else if(data instanceof Short) {
			return Bytes.toBytes((Short) data);
		}
		else if(data instanceof Byte) {
			return Bytes.toBytes((Byte) data);
		}
		else if(data instanceof Integer) {
			return Bytes.toBytes((Integer) data);
		}
		else if(data instanceof BigDecimal) {
			return Bytes.toBytes((BigDecimal) data);
		}
		else if(data instanceof byte[]) {
			return (byte[])data;
		}
		else if(data instanceof ByteBuffer) {
			return Bytes.toBytes((ByteBuffer)data);
		}
		else{
			return Bytes.toBytes(SimpleStringUtil.object2json(data));
		}


	}
	private Put convert(CommonRecord dbRecord){

		Map<String, Object> datas = dbRecord.getDatas();
		long timestamp = System.currentTimeMillis();
		Put put = new Put(toBytes(datas.get(hBaseOutputConfig.getRowKeyField())),timestamp);
		Iterator<Map.Entry<String,Object>> iterator = datas.entrySet().iterator();

		//如果没有设置列映射关系，则使用默认列
		if(hBaseOutputConfig.getFamilyColumnMappings() == null){
			while (iterator.hasNext()) {
				Map.Entry<String, Object> entry = iterator.next();
				byte[] v = toBytes(entry.getValue());
				put.addColumn(hBaseOutputConfig.getBglobalFamiliy(), Bytes.toBytes(entry.getKey()), timestamp, v);
			}
		}
		else {
			while (iterator.hasNext()) {
				Map.Entry<String, Object> entry = iterator.next();
				List<FamilyColumnMapping> familyColumnMappings = hBaseOutputConfig.getFamilyColumnMappings(entry.getKey());
				if (SimpleStringUtil.isNotEmpty(familyColumnMappings)) {
					byte[] v = toBytes(entry.getValue());
					for (FamilyColumnMapping familyColumnMapping : familyColumnMappings) {
						put.addColumn(familyColumnMapping.getBfamily(), familyColumnMapping.getBcolumn(), timestamp, v);
					}
				}
			}
		}
		return put;
	}
	public String execute(){
        if(records.size() > 0) {
            if (this.importContext.getMaxRetry() > 0) {
                if (this.tryCount >= this.importContext.getMaxRetry())
                    throw new TaskFailedException("task execute failed:reached max retry times " + this.importContext.getMaxRetry());
            }
            this.tryCount++;
            HBaseHelper hBaseHelper = null;
            try {

                hBaseHelper = HBaseHelperFactory.getHBaseHelper(hBaseOutputConfig.getName());

                List<Put> resources = new ArrayList<>();
                for (CommonRecord dbRecord : records) {
                    CommonRecord record = dbRecord;
                    Put basicDBObject = convert(dbRecord);
                    resources.add(basicDBObject);

                }
                if (resources.size() > 0) {
                    hBaseHelper.put(hBaseOutputConfig.getHbaseTable(), resources);
                   
                }
                

            } catch (Exception e) {

                throw ImportExceptionUtil.buildDataImportException(importContext, taskInfo, e);

            } catch (Throwable e) {

                throw ImportExceptionUtil.buildDataImportException(importContext, taskInfo, e);

            } 
            
        }
        else {
            if (logger.isInfoEnabled()){
                logger.info("All output data is ignored and do nothing.");
            }
        }
        finishTask();
        return null;
	}

	public int getTryCount() {
		return tryCount;
	}


}
