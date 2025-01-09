package org.frameworkset.tran.plugin.multi.output;

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class MultiOutPutDataTran extends AbstraCommonRecordOutPutDataTran {

    private MultiOutputConfig multiOutputConfig;
	protected String taskInfo ;
    private List<BaseDataTran> baseDataTrans;
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo + " start.");
	}




    @Override
	public void init(){
		super.init();
		taskInfo = new StringBuilder().append("Import data to MultiOutPut").toString();
	}


    public MultiOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
        super(taskContext,jdbcResultSet,importContext,  currentStatus);
        this.multiOutputConfig = (MultiOutputConfig) importContext.getOutputConfig();
        
        this.countDownLatch = countDownLatch;
    }
 
	public MultiOutPutDataTran(BaseDataTran baseDataTran) {
		super(baseDataTran);
	}
    @Override
    public void initTran(){
        super.initTran();
        MultiOutputDataTranPlugin multiOutputDataTranPlugin = (MultiOutputDataTranPlugin) outputPlugin;
        List<OutputPlugin> outputPlugins = multiOutputDataTranPlugin.getOutputPlugins();
        baseDataTrans = new ArrayList<>();
        for(OutputPlugin outputPlugin:outputPlugins) {
            BaseDataTran baseDataTran = outputPlugin.createBaseDataTran(this);            
            baseDataTran.setOutputPlugin(outputPlugin);
            baseDataTran.init();
            baseDataTrans.add(baseDataTran);
        }
    }
    @Override
    public  TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        MultiTaskCommandImpl taskCommand = new MultiTaskCommandImpl(taskCommandContext, (records,executorService) -> {
            List<Future> tasks = new ArrayList<>();
            for(BaseDataTran baseDataTran:baseDataTrans){
                TaskCommand taskCommand1 = baseDataTran.buildTaskCommand(taskCommandContext);
                taskCommand1.setRecords(records);
                taskCommandContext.addMultiOutputTask(executorService,tasks,taskCommand1);
            }
            return tasks;
        },outputPlugin.getOutputConfig());
        return taskCommand;
    }
    @Override
    public void stop(boolean fromException){
        for(BaseDataTran baseDataTran:baseDataTrans){
            baseDataTran.stop(fromException);
        }
        super.stop(fromException);
//        if(dataTranStopped)
//            return;
//        sendFile();//串行执行时，sendFile将不起作用
//        super.stop(fromException);
    }

    @Override
    public void stop2ndClearResultsetQueue(boolean fromException){
        for(BaseDataTran baseDataTran:baseDataTrans){
            baseDataTran.stop2ndClearResultsetQueue(fromException);
        }
        super.stop2ndClearResultsetQueue(fromException);
    }

}
