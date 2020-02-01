package org.frameworkset.nosql.hbase;
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/1/30 12:12
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseHelperTest {
	@Test
	public void testHBaseHelper(){
		Map<String,String> properties = new HashMap<String, String>();

		properties.put("hbase.zookeeper.quorum","192.168.137.133");
		properties.put("hbase.zookeeper.property.clientPort","2183");
		properties.put("zookeeper.znode.parent","/hbase");
		properties.put("hbase.ipc.client.tcpnodelay","true");
		properties.put("hbase.rpc.timeout","10000");
		properties.put("hbase.client.operation.timeout","10000");
		properties.put("hbase.ipc.client.socket.timeout.read","20000");
		properties.put("hbase.ipc.client.socket.timeout.write","30000");


		TableFactory tableFactory = HBaseHelper.buildTableFactory(properties,100,100,0L,1000l,1000,true,true);
		Table table = tableFactory.getTable(TableName.valueOf("AgentInfo"));
		Scan scan = new Scan();
		scan.setFilter(null);
		try {
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
//				System.out.println("获得到rowkey:" + new String(result.getRow()));
				List<Cell> cells= result.listCells();

//				for (Cell cell : cells) {
//
//					String row = Bytes.toString(result.getRow());
//
//					String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
//
//					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
//
//					String value = Bytes.toString(CellUtil.cloneValue(cell));
//
//					System.out.println("[row:"+row+"],[family:"+family1+"],[qualifier:"+qualifier+"]"+ ",[value:"+value+"],[time:"+cell.getTimestamp()+"]");
//
//				}
				byte[] rowKey = result.getRow();
				String agentId = BytesUtils.safeTrim(BytesUtils.toString(rowKey, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
				long reverseStartTime = BytesUtils.bytesToLong(rowKey, HBaseTables.AGENT_NAME_MAX_LEN);
				long startTime = TimeUtils.recoveryTimeMillis(reverseStartTime);

				byte[] serializedAgentInfo = result.getValue(HBaseTables.AGENTINFO_CF_INFO, HBaseTables.AGENTINFO_CF_INFO_IDENTIFIER);
				byte[] serializedServerMetaData = result.getValue(HBaseTables.AGENTINFO_CF_INFO, HBaseTables.AGENTINFO_CF_INFO_SERVER_META_DATA);
				byte[] serializedJvmInfo = result.getValue(HBaseTables.AGENTINFO_CF_INFO, HBaseTables.AGENTINFO_CF_INFO_JVM);

				final AgentInfoBo.Builder agentInfoBoBuilder = createBuilderFromValue(serializedAgentInfo);
				agentInfoBoBuilder.setAgentId(agentId);
				agentInfoBoBuilder.setStartTime(startTime);

				if (serializedServerMetaData != null) {
					agentInfoBoBuilder.setServerMetaData(new ServerMetaDataBo.Builder(serializedServerMetaData).build());
				}
				if (serializedJvmInfo != null) {
					agentInfoBoBuilder.setJvmInfo(new JvmInfoBo(serializedJvmInfo));
				}
				AgentInfo agentInfo = new AgentInfo(agentInfoBoBuilder.build());
				System.out.println(SimpleStringUtil.object2json(agentInfo));
			}
		}
		catch (Throwable e){
			e.printStackTrace();
		}
	}


	private AgentInfoBo.Builder createBuilderFromValue(byte[] serializedAgentInfo) {
		final Buffer buffer = new FixedBuffer(serializedAgentInfo);
		final AgentInfoBo.Builder builder = new AgentInfoBo.Builder();
		builder.setHostName(buffer.readPrefixedString());
		builder.setIp(buffer.readPrefixedString());
		builder.setPorts(buffer.readPrefixedString());
		builder.setApplicationName(buffer.readPrefixedString());
		builder.setServiceTypeCode(buffer.readShort());
		builder.setPid(buffer.readInt());
		builder.setAgentVersion(buffer.readPrefixedString());
		builder.setStartTime(buffer.readLong());
		builder.setEndTimeStamp(buffer.readLong());
		builder.setEndStatus(buffer.readInt());
		// FIXME - 2015.09 v1.5.0 added vmVersion (check for compatibility)
		if (buffer.hasRemaining()) {
			builder.setVmVersion(buffer.readPrefixedString());
		}
		return builder;
	}
}
