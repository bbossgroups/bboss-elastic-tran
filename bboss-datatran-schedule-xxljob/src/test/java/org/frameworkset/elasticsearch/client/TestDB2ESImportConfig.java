package org.frameworkset.elasticsearch.client;

import com.frameworkset.common.poolman.util.SQLUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.junit.Test;

public class TestDB2ESImportConfig {

	private void initDBSource(){
		SQLUtil.startNoPool("test",//数据源名称
				"com.mysql.cj.jdbc.Driver",//oracle驱动
				"jdbc:mysql://localhost:3306/bboss?allowPublicKeyRetrieval=true",//mysql链接串
				"root","123456",//数据库账号和口令
				"select 1 " //数据库连接校验sql
		);
	}

	@Test
	public void testDB2ESImportBuilder(){
		ImportBuilder importBuilder = ImportBuilder.newInstance();
		try {
			//清除测试表
			ElasticSearchHelper.getRestClientUtil().dropIndice("dbclobdemo");
		}
		catch (Exception e){

		}
		//数据源相关配置，可选项，可以在外部启动数据源
		DBInputConfig dbInputConfig = new DBInputConfig();
		dbInputConfig.setDbName("test")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
				.setDbUrl("jdbc:mysql://localhost:3306/bboss?allowPublicKeyRetrieval=true")
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(false);//是否使用连接池


		//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑
		dbInputConfig.setSql("select * from td_cms_document");
		importBuilder.setInputConfig(dbInputConfig);
		/**
		 * es相关配置
		 */
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
		elasticsearchOutputConfig
				.setIndex("dbclobdemo") //必填项
//				.setIndexType("dbclobdemo") //必填项
				.setEsIdField("documentId")//可选项
				.setEsParentIdField(null) //可选项,如果不指定，es自动为文档产生id
				.setRoutingValue(null) //可选项		importBuilder.setRoutingField(null);
				.setEsDocAsUpsert(true)//可选项
				.setEsRetryOnConflict(3)//可选项
				.setEsReturnSource(false)//可选项
				.setEsVersionField(null)//可选项
				.setEsVersionType(null)//可选项
				.setRefreshOption(null);//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");

		importBuilder.setOutputConfig(elasticsearchOutputConfig)
				.setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") //可选项,默认日期格式
				.setLocale("zh_CN")  //可选项,默认locale
				.setTimeZone("Etc/UTC");  //可选项,默认时区

		importBuilder.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，例如:doc_id -> docId

				.setBatchSize(1000);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

		/**
		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
		 */
		importBuilder.addFieldMapping("document_id","docId")
					 .addFieldMapping("docwtime","docwTime")
					 .addIgnoreFieldMapping("channel_id");//添加忽略字段

		/**
		 * 执行数据库表数据导入es操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();
	}

	@Test
	public void testSimpleDB2ESImportBuilder(){
		ImportBuilder importBuilder = ImportBuilder.newInstance();
		try {
			//清除测试表
			ElasticSearchHelper.getRestClientUtil().dropIndice("dbclobdemo");
		}
		catch (Exception e){

		}
		//数据源相关配置，可选项，可以在外部启动数据源
		DBInputConfig dbInputConfig = new DBInputConfig();
		dbInputConfig.setDbName("test")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
				.setDbUrl("jdbc:mysql://localhost:3306/bboss?allowPublicKeyRetrieval=true")
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setUsePool(false).setSql("select * from td_cms_document");//是否使用连接池


		//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑
		importBuilder.setInputConfig(dbInputConfig);
		/**
		 * es相关配置
		 */
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();


		/**
		 * es相关配置
		 */
		elasticsearchOutputConfig
				.setIndex("dbclobdemo") //必填项
				.setIndexType("dbclobdemo") //必填项
				.setRefreshOption(null);//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
		importBuilder.setOutputConfig(elasticsearchOutputConfig)
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，例如:doc_id -> docId
				.setBatchSize(1000);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理


		/**
		 * 执行数据库表数据导入es操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();
	}

	/**
	 * 从外部application.properties文件中加载数据源配置和es配置
	 */
	@Test
	public void testSimpleDB2ESImportBuilderFromExternalDBConfig(){
		ImportBuilder importBuilder = ImportBuilder.newInstance();
		try {
			//清除测试表
			ElasticSearchHelper.getRestClientUtil().dropIndice("dbclobdemo");
		}
		catch (Exception e){

		}


		//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑
		DBInputConfig dbInputConfig = new DBInputConfig();
		dbInputConfig.setSql("select * from td_cms_document");
		importBuilder.setInputConfig(dbInputConfig);
		/**
		 * es相关配置
		 */
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();


		elasticsearchOutputConfig
				.setIndex("dbclobdemo") //必填项
				.setIndexType("dbclobdemo") //必填项
				.setRefreshOption(null);//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
		importBuilder.setOutputConfig(elasticsearchOutputConfig)
				.setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，例如:doc_id -> docId
				.setBatchSize(1000);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理


		/**
		 * 执行数据库表数据导入es操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();
	}
}
