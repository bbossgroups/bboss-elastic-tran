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





	public void init(){
		super.init();
		taskInfo = new StringBuilder().append("Import data to MultiOutPut.").toString();
	}


    public MultiOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
        super(taskContext,jdbcResultSet,importContext,  currentStatus);
        this.multiOutputConfig = (MultiOutputConfig) importContext.getOutputConfig();
        
        this.countDownLatch = countDownLatch;
    }
 
	public MultiOutPutDataTran(BaseDataTran baseDataTran) {
		super(baseDataTran);
	}
    public void initTran(){
        super.initTran();
        MultiOutputDataTranPlugin multiOutputDataTranPlugin = (MultiOutputDataTranPlugin) outputPlugin;
        List<OutputPlugin> outputPlugins = multiOutputDataTranPlugin.getOutputPlugins();
        baseDataTrans = new ArrayList<>();
        for(OutputPlugin outputPlugin:outputPlugins) {
            BaseDataTran baseDataTran = outputPlugin.createBaseDataTran(this);
            baseDataTran.setOutputPlugin(outputPlugin);
            baseDataTrans.add(baseDataTran);
        }
    }
    @Override
    public  TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        MultiTaskCommandImpl taskCommand = new MultiTaskCommandImpl(taskCommandContext, (records,executorService) -> {
            List<Future> tasks = new ArrayList<>();
            for(BaseDataTran baseDataTran:baseDataTrans){
                TaskCommand taskCommand1 = baseDataTran.buildTaskCommand(taskCommandContext);
                taskCommand1.setMultiOutputTran(true);
                taskCommand1.setRecords(records);
                taskCommandContext.addMultiOutputTask(executorService,tasks,taskCommand1);
            }
            return tasks;
        },outputPlugin.getOutputConfig());
        return taskCommand;
    }


}
