package org.frameworkset.tran.plugin.hbase.input;
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

import com.frameworkset.orm.annotation.BatchContext;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.nosql.hbase.HBaseHelper;
import org.frameworkset.nosql.hbase.TableFactory;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.hbase.HBaseRecord;
import org.frameworkset.tran.hbase.HBaseRecordContextImpl;
import org.frameworkset.tran.hbase.HBaseResultSet;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public  class HBaseInputDatatranPlugin extends BaseInputPlugin {
	private TableFactory tableFactory;
	private static Logger logger = LoggerFactory.getLogger(HBaseInputDatatranPlugin.class);

	private HBaseInputConfig hBaseInputConfig;
	private byte[] incrementFamily;
	private byte[] incrementColumn;
	@Override
	public void init(){


	}
	@Override
	public Context buildContext(TaskContext taskContext, TranResultSet tranResultSet, BatchContext batchContext){
		return new HBaseRecordContextImpl(  taskContext,importContext, tranResultSet, batchContext);

	}


	public HBaseInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		hBaseInputConfig = (HBaseInputConfig) importContext.getInputConfig();
	}
	@Override
	public void destroy(boolean waitTranStop){
		
		HBaseHelper.destroy();

	}
	@Override
	public void beforeInit() {
		initHBase();



	}
	protected void doTran(ResultScanner rs,TaskContext taskContext){
		HBaseResultSet hBaseResultSet = new HBaseResultSet(importContext,rs);
		BaseDataTran baseDataTran = dataTranPlugin.createBaseDataTran(taskContext,hBaseResultSet,null);
		baseDataTran.initTran();
		baseDataTran.tran();
	}

	protected void initHBase() {
//		MongoDBConfig mongoDBConfig = new MongoDBConfig();
//		mongoDBConfig.setName(es2DBContext.getName());
//		mongoDBConfig.setCredentials(es2DBContext.getCredentials());
//		mongoDBConfig.setServerAddresses(es2DBContext.getServerAddresses());
//		mongoDBConfig.setOption(es2DBContext.getOption());//private String option;
//		mongoDBConfig.setWriteConcern(es2DBContext.getWriteConcern());//private String writeConcern;
//		mongoDBConfig.setReadPreference(es2DBContext.getReadPreference());//private String readPreference;
//		mongoDBConfig.setAutoConnectRetry(es2DBContext.getAutoConnectRetry());//private Boolean autoConnectRetry = true;
//
//		mongoDBConfig.setConnectionsPerHost(es2DBContext.getConnectionsPerHost());//private int connectionsPerHost = 50;
//
//		mongoDBConfig.setMaxWaitTime(es2DBContext.getMaxWaitTime());//private int maxWaitTime = 120000;
//		mongoDBConfig.setSocketTimeout(es2DBContext.getSocketTimeout());//private int socketTimeout = 0;
//		mongoDBConfig.setConnectTimeout(es2DBContext.getConnectTimeout());//private int connectTimeout = 15000;
//
//
//		/**是否启用sql日志，true启用，false 不启用，*/
//		mongoDBConfig.setThreadsAllowedToBlockForConnectionMultiplier(es2DBContext.getThreadsAllowedToBlockForConnectionMultiplier());//private int threadsAllowedToBlockForConnectionMultiplier;
//		mongoDBConfig.setSocketKeepAlive(es2DBContext.getSocketKeepAlive());//private Boolean socketKeepAlive = false;
//
//		mongoDBConfig.setMode( es2DBContext.getMode());
//		MongoDBHelper.init(mongoDBConfig);
//		HbaseConfigurationFactoryBean hbaseConfigurationFactoryBean = new HbaseConfigurationFactoryBean();
//		Configuration configuration = hbaseConfigurationFactoryBean.getConfiguration();
//		ExecutorService executorService = ThreadPoolFactory.buildThreadPool("HBase-Input","HBase tran",100,100,0L,1000l,1000,true,true);
//		ConnectionFactoryBean connectionFactoryBean = new ConnectionFactoryBean(configuration,executorService);
//		try {
//			tableFactory = new HbaseTableFactory(connectionFactoryBean.getConnection());
//		} catch (Exception e) {
//			throw new HBaseTranException(e);
//		}
		 HBaseHelper.buildHBaseClient(hBaseInputConfig.getHbaseClientProperties(),
				 hBaseInputConfig.getHbaseClientThreadCount(),
				 hBaseInputConfig.getHbaseClientThreadQueue(),
				 hBaseInputConfig.getHbaseClientKeepAliveTime(),
				 hBaseInputConfig.getHbaseClientBlockedWaitTimeout(),
				 hBaseInputConfig.getHbaseClientWarnMultsRejects(),
				 hBaseInputConfig.isHbaseClientPreStartAllCoreThreads(),
				 hBaseInputConfig.getHbaseClientThreadDaemon(),hBaseInputConfig.getHbaseAsynMetricsEnable());
		tableFactory = HBaseHelper.getTableFactory();
	}
	@Override
	public void afterInit(){
		initIncrementInfos();
	}

	@Override
	public void initStatusTableId() {
		if(importContext.isIncreamentImport()) {
			//计算增量记录id
			String statusTableId = hBaseInputConfig.getHbaseTable();
			if(hBaseInputConfig.getIncrementFamilyName() != null)
				statusTableId =statusTableId +"|"+hBaseInputConfig.getIncrementFamilyName();
			if(importContext.getLastValueColumnName() != null)
				statusTableId =statusTableId +"|"+importContext.getLastValueColumnName();

			if(hBaseInputConfig.getStartRow() != null )
				statusTableId =statusTableId +"|"+hBaseInputConfig.getStartRow();
			if(hBaseInputConfig.getEndRow() != null )
				statusTableId =statusTableId +"|"+hBaseInputConfig.getEndRow();
//			if(hBaseInputConfig.getScanFilters() != null )
//				statusTableId =statusTableId +"|"+hBaseInputConfig.getScanFilters();
			importContext.setStatusTableId(statusTableId.hashCode());

		}

	}

	private void exportESData(TaskContext taskContext){

		Table table = null;
		try {
			table = tableFactory.getTable(TableName.valueOf(hBaseInputConfig.getHbaseTable()));
			Scan scan = new Scan();
			if(hBaseInputConfig.getStartRow() != null){
				scan.setStartRow(Bytes.toBytes(hBaseInputConfig.getStartRow()));
			}
			if(hBaseInputConfig.getEndRow() != null){
				scan.setStopRow(Bytes.toBytes(hBaseInputConfig.getEndRow()));
			}
			if(hBaseInputConfig.getHbaseBatch() != null){
				scan.setBatch(hBaseInputConfig.getHbaseBatch());
			}
			if(hBaseInputConfig.getMaxResultSize() != null){
				scan.setMaxResultSize(hBaseInputConfig.getMaxResultSize());
			}



			if(hBaseInputConfig.getStartTimestamp() != null && hBaseInputConfig.getEndTimestamp() != null)
				scan.setTimeRange(hBaseInputConfig.getStartTimestamp(),hBaseInputConfig.getEndTimestamp());
			if (importContext.isIncreamentImport()) {

				putLastParamValue(scan);

			}
			else{
				if(hBaseInputConfig.getFilterList() != null){
					scan.setFilter(hBaseInputConfig.getFilterList());
				}
				else if(hBaseInputConfig.getFilter() != null){
					scan.setFilter(hBaseInputConfig.getFilter());
				}
			}


			if(importContext.getFetchSize() != null) {
				scan.setCaching(importContext.getFetchSize());
			}

			ResultScanner rs = table.getScanner(scan);
			doTran(rs,taskContext);
		}
		catch (Exception e){
			throw new DataImportException(e);
		}
		finally {
			if(tableFactory != null && table != null){
				tableFactory.releaseTable(table);
			}
		}


	}
	private void initIncrementInfos(){
		if(getLastValueVarName() != null && !getLastValueVarName().equals("_")){

			if(hBaseInputConfig.getIncrementFamilyName() != null){
				incrementFamily = Bytes.toBytes(hBaseInputConfig.getIncrementFamilyName());
				incrementColumn = Bytes.toBytes(getLastValueVarName());
			}
			else{
				byte[][] infos = HBaseRecord.parserColumn(getLastValueVarName());
				incrementFamily = infos[0];
				incrementColumn = infos[1];
			}
		}


	}
	private Long lastValue;
	@Override
	public Long getTimeRangeLastValue(){
		return lastValue;
	}
	public void putLastParamValue(Scan scan) throws IOException {
		Status currentStatus = dataTranPlugin.getCurrentStatus();
		if(dataTranPlugin.getLastValueType() == ImportIncreamentConfig.NUMBER_TYPE) {

			SingleColumnValueFilter scvf = new SingleColumnValueFilter(incrementFamily, incrementColumn,
					CompareFilter.CompareOp.GREATER, Bytes.toBytes((Long) currentStatus.getLastValue()));

			if (hBaseInputConfig.getFilterIfMissing() != null)
				scvf.setFilterIfMissing(hBaseInputConfig.getFilterIfMissing()); //默认为false， 没有此列的数据也会返回 ，为true则只返回name=lisi的数据
			if (hBaseInputConfig.getFilterList() != null) {
//				filterList.addFilter(scvf);
				FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				list.addFilter(hBaseInputConfig.getFilterList());
				list.addFilter(scvf);
				scan.setFilter(list);
			} else if(hBaseInputConfig.getFilter() != null){
				FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				list.addFilter(hBaseInputConfig.getFilter());
				list.addFilter(scvf);
				scan.setFilter(list);
			}
			else{
				scan.setFilter(scvf);
			}
		}
		else{
			if(hBaseInputConfig.isIncrementByTimeRange()){
				if(lastValue == null){

					lastValue = ((Date)currentStatus.getLastValue()).getTime();
				}
				long temp = System.currentTimeMillis();
				scan.setTimeRange(lastValue,temp);
				lastValue = temp;
				if(hBaseInputConfig.getFilterList() != null){
					scan.setFilter(hBaseInputConfig.getFilterList());
				}
				else if(hBaseInputConfig.getFilter() != null){
					scan.setFilter(hBaseInputConfig.getFilter());
				}
			}
			else {
				Object lv = null;
				if (currentStatus.getLastValue() instanceof Date) {
					lv = currentStatus.getLastValue();
//				params.put(getLastValueVarName(), this.currentStatus.getLastValue());
				} else {
					if (currentStatus.getLastValue() instanceof Long) {
						lv = new Date((Long) currentStatus.getLastValue());
					} else if (currentStatus.getLastValue() instanceof Integer) {
						lv = new Date(((Integer) currentStatus.getLastValue()).longValue());
					} else if (currentStatus.getLastValue() instanceof Short) {
						lv = new Date(((Short) currentStatus.getLastValue()).longValue());
					} else {
						lv = new Date(((Number) currentStatus.getLastValue()).longValue());
					}
				}
				SingleColumnValueFilter scvf = new SingleColumnValueFilter(incrementFamily, incrementColumn,

						CompareFilter.CompareOp.GREATER, Bytes.toBytes(((Date) currentStatus.getLastValue()).getTime()));

				if (hBaseInputConfig.getFilterIfMissing() != null)
					scvf.setFilterIfMissing(hBaseInputConfig.getFilterIfMissing()); //默认为false， 没有此列的数据也会返回 ，为true则只返回name=lisi的数据
				if (hBaseInputConfig.getFilterList() != null) {
//				filterList.addFilter(scvf);
					FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
					list.addFilter(hBaseInputConfig.getFilterList());
					list.addFilter(scvf);
					scan.setFilter(list);
				} else if(hBaseInputConfig.getFilter() != null){
					FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
					list.addFilter(hBaseInputConfig.getFilter());
					list.addFilter(scvf);
					scan.setFilter(list);
				}
				else{
					scan.setFilter(scvf);
				}
			}

		}
		if(importContext.isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(currentStatus.getLastValue()).toString());
		}
	}
	@Override
	public void doImportData(TaskContext taskContext)  throws DataImportException {


			try {
				exportESData(  taskContext);
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw new DataImportException(e);
			}

	}


}
