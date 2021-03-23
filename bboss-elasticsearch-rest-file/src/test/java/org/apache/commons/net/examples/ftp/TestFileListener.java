package org.apache.commons.net.examples.ftp;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.BaseDataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.es.input.ESImportContext;
import org.frameworkset.tran.input.file.*;
import org.frameworkset.tran.schedule.TaskContext;
import org.junit.Before;
import org.junit.Test;
/**
 * @author xutengfei
 * @description
 * @create 2021/3/15
 */
public class TestFileListener {
    @Before
    public void init(){
    }
    @Test
    public void start() throws Exception {
        FileImportConfig config = new FileImportConfig();
        //.*.txt.[0-9]+$
        config.addConfig("E:\\ELK\\data\\data1",".*.txt","");
        config.addConfig("E:\\ELK\\data\\data2",".*.txt","");
        config.addConfig("E:\\ELK\\data\\data3",".*.txt","^[0-9]{4}-[0-9]{2}-[0-9]{2}");
//        config.addConfig("D:\\ecslog\\",".*.20201022230056","");
        FileImportContext context = new FileImportContext(config);
        FileBaseDataTranPlugin fileBaseDataTranPlugin = new FileBaseDataTranPlugin(context,
                new ESImportContext(),
                new FileListener(new FileListenerService(context,null))){
            @Override
            protected BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet) {
                return null;
            }
        };
        fileBaseDataTranPlugin.beforeInit();
        fileBaseDataTranPlugin.afterInit();
        fileBaseDataTranPlugin.doImportData(null);
        return;
    }
}
