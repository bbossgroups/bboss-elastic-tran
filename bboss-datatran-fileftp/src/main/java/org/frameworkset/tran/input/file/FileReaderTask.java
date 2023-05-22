package org.frameworkset.tran.input.file;

import com.frameworkset.util.BaseSimpleStringUtil;
import com.frameworkset.util.FileUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.file.monitor.FileManager;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.record.FieldMappingUtil;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TranStopReadEOFCallback;
import org.frameworkset.tran.task.TranStopReadEOFCallbackContext;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class FileReaderTask extends FieldManager{
    private static Logger logger = LoggerFactory.getLogger(FileReaderTask.class);
    protected FileInfo fileInfo;
    protected FileInputConfig fileImportConfig;


    /**
     * 文件监听事件服务
     */
    protected FileListenerService fileListenerService;
    /**
     * 文件开始行标识正则
     */
    private Pattern pattern;
    private boolean rootLevel;
    protected boolean enableMeta;

    protected BaseDataTran fileDataTran;
    private RandomAccessFile raf ;
    //状态 0启用 1失效
    public static final int STATUS_OK = 0;
    public static final int STATUS_NO = 1;
    private int status = STATUS_OK;
    protected Status currentStatus;
    protected volatile boolean taskEnded;
    protected volatile boolean taskEndOrStop;

    protected volatile boolean interrupted;
    /**
     * jsondata：标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
     */
    private boolean jsondata ;
    protected Thread worker ;
    protected long oldLastModifyTime = -1L;
    protected long lastExecuteTime = -1L;

    protected long checkFileModifyInterval = 1000L;
    protected long closeOlderTime ;
    protected CloseOldedFileAssert closeOldedFileAssert;

    protected IgnoreFileAssert ignoreFileAssert;
    protected long ignoreOlderTime ;
    protected TaskContext taskContext;
    protected FileConfig fileConfig;

    protected boolean fileOlded;
    protected boolean fileCompleteErrored;
    protected boolean fileDeleted;


    /**
     * 文件采集偏移量
     */
    protected long pointer;

    public FileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig,
						  FileListenerService fileListenerService,
						  BaseDataTran fileDataTran,
						  Status currentStatus , FileInputConfig fileImportConfig ) {
        this.fileImportConfig = fileImportConfig;
        this.sleepAwaitTimeAfterFetch = fileImportConfig.getSleepAwaitTimeAfterFetch();
		this.sleepAwaitTimeAfterCollect = fileImportConfig.getSleepAwaitTimeAfterCollect();
        this.fileListenerService = fileListenerService;
        String charSet = fileConfig.getCharsetEncode() ;
        if(charSet == null || charSet.equals("")){
            charSet = this.fileListenerService.getFileInputConfig().getCharsetEncode();
        }
        this.pointer = 0;
        this.fileConfig = fileConfig;
        this.taskContext = taskContext;
        if(taskContext instanceof FileTaskContext) {

            this.fileInfo = ((FileTaskContext)taskContext).getFileInfo();
        }


        if(fileConfig.getFileHeadLineRexPattern() != null){
            pattern = fileConfig.getFileHeadLineRexPattern() ;
        }
        closeOlderTime = fileConfig.getCloseOlderTime() == null?0:fileConfig.getCloseOlderTime();
        ignoreOlderTime = fileConfig.getIgnoreOlderTime() == null?0:fileConfig.getIgnoreOlderTime();
        this.closeOldedFileAssert = fileConfig.getCloseOldedFileAssert();
        this.ignoreFileAssert = fileConfig.getIgnoreFileAssert();
        rootLevel = this.fileListenerService.getFileInputConfig().isRootLevel();
        jsondata = this.fileListenerService.getFileInputConfig().isJsondata();
        enableMeta = this.fileListenerService.getFileInputConfig().isEnableMeta();
        checkFileModifyInterval = this.fileListenerService.getFileInputConfig().getCheckFileModifyInterval();
        this.fileDataTran = fileDataTran;

        this.currentStatus = currentStatus;


    }
    public FileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig ) {
        this.fileImportConfig = fileImportConfig;
        this.currentStatus = currentStatus;
        this.fileInfo = new FileInfo( fileId);
        this.sleepAwaitTimeAfterFetch = fileImportConfig.getSleepAwaitTimeAfterFetch();
		this.sleepAwaitTimeAfterCollect = fileImportConfig.getSleepAwaitTimeAfterCollect();
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    /**
     * 检测文件是否被重命名，如果重命名则标识文件还存在
     * 1 文件被删除
     * 2 文件被重命名，能够识别本目录下面重命名的文件
     * -1 未知状态  判断过程中出错了，返回未知状态
     * @param logFileId
     * @return
     */
    public int fileExist(String logFileId){
        FileConfig fileConfig = fileInfo.getFileConfig();
        if(!fileInfo.getFileConfig().isEnableInode()){
            return 1;
        }
        File logDir = fileConfig.getLogDir();
        FilenameFilter filter = fileConfig.getFilter();
        try {
            if (logDir.isDirectory() && logDir.exists()) {
                File[] files = logDir.listFiles(filter);
                File file = null;
                for (int i = 0; files != null && i < files.length; i++) {
                    file = files[i];
                    String fileId = FileInodeHandler.inode(file,fileConfig.isEnableInode());
                    if(fileId.equals(logFileId) ){
                        //文件存在，被重命名了
                        return 2;
                    }
                }
            }
            //文件被删除了
            return 1;
        }
        catch(Exception e){
            //判断过程中出错了，返回未知状态
            return -1;
        }

    }

    /**
     * 判断文件是否被重命名
     * @param logFile
     * @return
     */
    public boolean fileRenamed(File logFile){
        FileConfig fileConfig = fileInfo.getFileConfig();
        if(!fileConfig.isEnableInode()){
            return false;
        }
        String logFileId = FileInodeHandler.inode(logFile,fileConfig.isEnableInode());
        return  !fileInfo.getFileId().equals(logFileId);

    }

    public FileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
                          Status currentStatus , FileInputConfig fileImportConfig   ) {
        this(  taskContext,file,fileId,  fileConfig,fileListenerService,fileDataTran,currentStatus,  fileImportConfig );
        this.pointer = pointer;
    }
    protected void registEndJob(){
        fileDataTran.setTranStopReadEOFCallback(new TranStopReadEOFCallback() {



            @Override
            public void call(TranStopReadEOFCallbackContext tranStopReadEOFCallbackContext) {

                try {
//                    if (taskEndOrStop) {
                        File file = fileInfo.getFile();
                        boolean removed = false;
                        if(tranStopReadEOFCallbackContext.isReachEOFClose()) {

                            if(tranStopReadEOFCallbackContext.getException() == null) {
                                if (logger.isInfoEnabled())
                                    logger.info("{} reached eof and will be closed.", FileReaderTask.this.toString());
                                fileListenerService.moveTaskToComplete(FileReaderTask.this);
                                removed = true;
                                try {
                                    if (fileImportConfig.isBackupSuccessFiles())//备份采集完的数据文件，默认保留一周，过期清理
                                        backupFile(currentStatus.getRelativeParentDir(), fileInfo.getFile());
                                    else if (fileConfig.isDeleteEOFFile())//删除文件
                                    {
                                        if (logger.isInfoEnabled())
                                            logger.info("delete {} on reached eof file.", file.getCanonicalPath());
                                        file.delete();

                                    }
                                } catch (Exception e) {
                                    logger.warn("", e);
                                }
                            }
                            else {

                                if (logger.isInfoEnabled())
                                    logger.info("Move failed collect file to Failed Tasks {}. ", FileReaderTask.this.toString());
                                fileListenerService.removeFailedTask(FileReaderTask.this);
                                removed = true;
                            }

                        }
//                        else {

                        if(fileDeleted){
                            if(logger.isInfoEnabled())
                                logger.info("文件[{}]被删除，终止作业，只清理作业任务，停止转换通道，清理和销毁通道任务上下文，当文件回来或者文件恢复后重新分配新的采集通道采集数据",fileInfo.getFilePath());
                            fileListenerService.doDelete(fileInfo.getFileId());

                        }
                        else if(tranStopReadEOFCallbackContext.getException() != null){
                            if(!removed) {
                                if (logger.isInfoEnabled())
                                    logger.info("Move failed collect file to Failed Tasks {}. tranStopReadEOFCallbackContext.getException() is not null.", FileReaderTask.this.toString());
                                fileListenerService.removeFailedTask(FileReaderTask.this);
                                removed = true;
                            }
                        }
                        else if(fileOlded){
                            if(logger.isInfoEnabled())
                                logger.info("文件[{}]超过静默时间或者空闲时间内容未变化，完成采集作业",fileInfo.getFilePath());
                            fileListenerService.addOldedFileTask(currentStatus.getFileId(),new FileReaderTask(currentStatus.getFileId()
                                    ,currentStatus,fileImportConfig));
//                            fileDataTran.getDataTranPlugin().handleOldedTask(currentStatus);
                        }
                        else if(fileCompleteErrored){
                            if(!removed) {
                                if (logger.isInfoEnabled())
                                    logger.info("Move failed collect file to Failed Tasks {}. ", FileReaderTask.this.toString());
                                fileListenerService.removeFailedTask(FileReaderTask.this);
                                removed = true;
                            }
                        }

                        else if(interrupted){
                            if(logger.isInfoEnabled())
                                logger.info("文件[{}]采集被中断，终止作业，只清理作业任务，停止转换通道，清理和销毁通道任务上下文，当文件回来或者文件恢复后重新分配新的采集通道采集数据",fileInfo.getFilePath());
                            fileListenerService.doDelete(fileInfo.getFileId());//是否需要专门的中断文件管理队列，待定
                        }

//                        }

//                    }

                }
                finally {
                    fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
                    destroyTaskContext();
                }

            }
        });
    }
    public void start(){
        String threadName = null;
        if(fileConfig.isEnableInode()) {
            threadName = "FileReaderTask-Thread|" + fileInfo.getFilePath() + "|" + fileInfo.getFileId();
        }
        else{
            threadName = "FileReaderTask-Thread|"+fileInfo.getFilePath() ;

        }
        registEndJob();
//        worker.setDaemon(true);
        worker = new Thread(new Work(),threadName );
        if(logger.isInfoEnabled())
            logger.info(threadName+" started.Current Status is "+currentStatus.toString());
        worker.start();

    }
    public void interruptWork(){
        if(worker != null){
            worker.interrupt();
            try {
                worker.join();
            } catch (InterruptedException e) {
//                e.printStackTrace();
            }
        }
    }

    public String getFilePath() {
        return fileInfo.getFilePath();
    }
    public boolean isEnableInode(){
        return fileInfo.getFileConfig().isEnableInode();
    }

    public class Work implements Runnable{

        @Override
        public void run() {
            logger.info("文件内容{}采集任务开始.",
                    fileInfo.getFilePath());
            boolean delete = false;
            boolean olded = false;
            boolean errorComplete = false;
            File file = fileInfo.getFile();
            String fileId = fileInfo.getFileId();
            long pauseScheduleTimeStamp = 0L;
            DataTranPlugin dataTranPlugin = fileListenerService.getBaseDataTranPlugin();
            InputPlugin inputPlugin = dataTranPlugin.getInputPlugin();
            boolean _taskEndOrStop = false;
            do {
                try {
                    _taskEndOrStop = taskEnded || dataTranPlugin.checkTranToStop() || inputPlugin.isStopCollectData();
                    if (_taskEndOrStop) {
                        logger.info("退出文件内容采集循环：taskEnded={}，dataTranPlugin.checkTranToStop = {},inputPlugin.isStopCollectData={},file={}",
                                taskEnded,dataTranPlugin.checkTranToStop(),inputPlugin.isStopCollectData(),fileInfo.getFilePath());
                        break;
                    }
                    if (file.exists()) {

                        long lastModifyTime = FileManager.getFileLastTimestamp(file);
                        if (oldLastModifyTime == -1L) {
                            oldLastModifyTime = lastModifyTime;
                            long s = System.currentTimeMillis();
                            execute();
                            long end = System.currentTimeMillis();
                            lastExecuteTime = end - s;
                            continue;
                        } else if (oldLastModifyTime == lastModifyTime) {
                            long idleTime = System.currentTimeMillis() - oldLastModifyTime;
                            if (pauseScheduleTimeStamp > 0L) {//计算暂停时间,并将暂停时间从空闲时间中剔除
                                long pauseScheduleTime = System.currentTimeMillis() - pauseScheduleTimeStamp;
                                idleTime = idleTime - pauseScheduleTime;
                            }
                            if (closeOlderTime > 0L && idleTime >= closeOlderTime) {//已经超过指定的最大空闲静默时间，停止文件监控作业

                                if (closeOldedFileAssert == null) {

                                    olded = true;
                                } else {
                                    olded = closeOldedFileAssert.canClose(fileInfo);
                                    if (!olded) {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("文件[新：{}|{}，老：{}],idleTime:{},内容超过{}毫秒未变化，已经超过指定的最大空闲静默时间closeOlderTime，停止本文件采集作业,olded:{}",
                                                    fileInfo.getFilePath(), fileInfo.getFileId(), fileInfo.getOriginFilePath(), idleTime, closeOlderTime, olded);
                                        }
                                    }

                                }
                                if (olded) {
                                    if (logger.isInfoEnabled())
                                        logger.info("文件[{}|{}]idleTime:{},内容超过{}毫秒未变化，已经超过指定的最大空闲静默时间closeOlderTime，停止本文件采集作业.",
                                                fileInfo.getFilePath(), fileInfo.getFileId(), idleTime, closeOlderTime);
                                    break;
                                }
                            }
                            if (ignoreOlderTime > 0L && idleTime >= ignoreOlderTime) {//已经超过指定的最大空闲静默时间，停止文件监控作业
//                            logger.info("file[{}|{}] idleTime:{},ignoreOlderTime:{}",fileInfo.getFilePath(),fileInfo.getFileId(),idleTime,ignoreOlderTime);


                                if (ignoreFileAssert == null) {

                                    olded = true;
                                } else {
                                    olded = ignoreFileAssert.canIgnore(fileInfo);
                                }
                                if (olded) {
                                    if (logger.isInfoEnabled())
                                        logger.info("文件[{}|{}]idleTime:{},内容超过{}毫秒未变化，已经超过指定的最大空闲静默时间ignoreOlderTime，停止本文件采集作业.",
                                                fileInfo.getFilePath(), fileInfo.getFileId(), idleTime, ignoreOlderTime);
                                    break;
                                }
                            }
                            if (closeOldedFileAssert != null) {
                                olded = closeOldedFileAssert.canClose(fileInfo);
                                if (olded) {
                                    logger.info("文件[新：{}|{},老:{}]内容已经采集完毕，停止本文件采集作业.",
                                            fileInfo.getFilePath(), fileInfo.getFileId(), fileInfo.getOriginFilePath());
                                    break;
                                }
                            }
                            if (ignoreFileAssert != null) {
                                olded = ignoreFileAssert.canIgnore(fileInfo);
                                if (olded) {
                                    logger.info("文件[新：{}|{},老:{}]内容已经采集完毕，停止本文件采集作业.",
                                            fileInfo.getFilePath(), fileInfo.getFileId(), fileInfo.getOriginFilePath());
                                    break;
                                }
                            }
                            try {
                                sleep(checkFileModifyInterval);
                            } catch (InterruptedException e) {
                                if(logger.isDebugEnabled())
                                    logger.debug("文件内容采集线程checkFileModifyInterval休眠期中断异常："+fileInfo.getFilePath(),e);
                                break;
                            }
                            continue;
                        } else {
                            boolean pauseSchedule = fileListenerService.isSchedulePaussed(false);//这里需要关闭采集自动暂停机制，不能影响其他并行采集的文件调度
                            if (pauseSchedule) {
                                pauseScheduleTimeStamp = System.currentTimeMillis();
                                if (logger.isInfoEnabled())
                                    logger.info("Ignore Paused FileReader[{}] Task,waiting for next resume schedule sign to continue.", fileInfo.getFilePath());
                                try {
                                    sleep(checkFileModifyInterval);
                                } catch (InterruptedException e) {
                                    if(logger.isDebugEnabled())
                                        logger.debug("文件内容采集线程checkFileModifyInterval休眠期中断异常："+fileInfo.getFilePath(),e);
                                    break;
                                }
                                continue;
                            } else {
                                pauseScheduleTimeStamp = 0L;
                            }
                            if (fileRenamed(file)) //文件重命名，等待文件被清理重新更新新的File对象
                            {
                                File fileIdFile = FileInodeHandler.getFileByInode(fileConfig, fileId);//查找重命名后的文件
                                if (fileIdFile != null) {//文件发生了重命名
                                    String filePath = FileInodeHandler.change(fileIdFile.getAbsolutePath());
                                    if (logger.isInfoEnabled())
                                        logger.info("Rename file {} to {}", fileInfo.getOriginFilePath(), filePath);
                                    changeFile(filePath, fileIdFile);
                                    file = fileIdFile;
                                    if (!fileInfo.isCloseEOF() && fileConfig.isCloseRenameEOF())
                                        fileInfo.setCloseEOF(true);//设置关闭标识，重命名的文件被采集完后关闭
                                } else {
                                    delete = true;
                                    break;
                                }

                            }
                            oldLastModifyTime = lastModifyTime;
                            long s = System.currentTimeMillis();
                            execute();
                            long end = System.currentTimeMillis();
                            lastExecuteTime = end - s;
                            if (sleepAwaitTimeAfterFetch <= 0L && sleepAwaitTimeAfterCollect > 0L) {
                                try {
                                    sleep(sleepAwaitTimeAfterCollect);//采集完毕后休息一会儿再继续
                                } catch (InterruptedException e) {
                                    if(logger.isDebugEnabled())
                                        logger.debug("文件内容采集线程sleepAwaitTimeAfterCollect休眠期中断异常："+fileInfo.getFilePath(),e);
                                    break;
                                }
                            }
//                        try {
//                            sleep(checkFileModifyInterval);//采集完毕后休息一会儿再继续
//                        } catch (InterruptedException e) {
//                            break;
//                        }
                            continue;
                        }
                    } else { //可能的删除文件，待处理
                        int lable = fileExist(fileId);//根据文件号识别文件是否被删除
                        if (lable == 1 || lable == -1)//文件被删除或未知
                        {

                            delete = true;
                            break;
                        }
                        try {
                            sleep(checkFileModifyInterval);
                        } catch (InterruptedException e) {
                            if(logger.isDebugEnabled())
                                logger.debug("文件内容采集线程checkFileModifyInterval休眠期中断异常："+fileInfo.getFilePath(),e);
                            break;
                        }
                        continue;
                    }
                }
                catch (Exception e){

                    logger.error("文件内容采集异常："+fileInfo.getFilePath(),e);
                    fileDataTran.getDataTranPlugin().throwException(getTaskContext(),e);
                    if(!fileDataTran.getDataTranPlugin().isContinueOnError()){
                        errorComplete = true;
                        break;
                    }
                }



            }while(true);
            if(delete){
                //文件被删除，只清理作业任务，停止转换通道，清理和销毁通道任务上下文，当文件回来或者文件恢复后重新分配新的采集通道采集数据
                if(logger.isInfoEnabled())
                    logger.info("文件[{}]被删除，停止文件内容采集，只清理作业任务，停止转换通道，清理和销毁通道任务上下文，当文件回来或者文件恢复后重新分配新的采集通道采集数据",fileInfo.getFilePath());
//                fileListenerService.doDelete(fileId);
                //need test
                /**
                fileListenerService.addCompletedFileTask(currentStatus.getFileId(),new FileReaderTask(currentStatus.getFileId()
                        ,currentStatus));
                fileDataTran.getDataTranPlugin().handleOldedTask(currentStatus);
                 */
                fileDeleted = true;
                taskEnded();

//                fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
//                destroyTaskContext();
            }
            else if(olded){
                if(logger.isInfoEnabled())
                    logger.info("文件[{}]超过静默时间或者空闲时间内容未变化，完成文件内容采集，等待内容处理完毕后终止文件采集通道。",fileInfo.getFilePath());
                fileOlded = true;
                try {
                    sendReadEOFcloseEvent(pointer);
                } catch (IOException e) {
                    logger.warn("sendReadEOFcloseEvent failed:",e);
                } catch (InterruptedException e) {
                    logger.warn("sendReadEOFcloseEvent failed:",e);
                }
                taskEnded();


//                fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
//                destroyTaskContext();
            }
            else if(errorComplete){ //dataTraan execute afterCall and destroyTaskContext
                if(logger.isInfoEnabled())
                    logger.info("文件[{}]采集异常，终止文件内容采集，等待内容处理完毕后终止文件采集通道",fileInfo.getFilePath());
                fileCompleteErrored = true;
                taskEnded();

//                fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
//                destroyTaskContext();
            }
            else{  //dataTraan execute afterCall and destroyTaskContext

                if(_taskEndOrStop) {
                    taskEndOrStop = _taskEndOrStop;
                    if(logger.isInfoEnabled()) {
                        if(taskEnded) {
                            logger.info("数据内容采集完成:{}，等待内容处理完毕后终止文件采集通道", fileInfo.getFilePath());
                        }
                        else{
                            logger.info("停止数据内容采集:{}，等待内容处理完毕后终止文件采集通道", fileInfo.getFilePath());
                        }
                    }
                }
                else{
                    interrupted = true;
                    if(logger.isInfoEnabled()) {
                        logger.info("文件[{}]任务中断，导致终止文件内容采集，等待内容处理完毕后终止文件采集通道", fileInfo.getFilePath());
                    }
                }

                taskEnded();
//                fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
//                destroyTaskContext();
            }
        }

    }

    public boolean isInterrupted() {
        return interrupted;
    }

    public boolean isTaskEndOrStop() {
        return taskEndOrStop;
    }

    class Line{
        private String line;
        private boolean eof;
        private boolean eol;
        Line(String line, boolean eof,boolean eol) {
            this.line = line;
            this.eof = eof;
            this.eol = eol;
        }

        public String getLine() {
            return line;
        }



        public boolean isEof() {
            return eof;
        }

        public boolean isRollbackPreLine(){
//            return eof && !eol && !fileInfo.getFileConfig().isCloseEOF();
			return eof && !eol && !fileInfo.isCloseEOF();
        }


    }
    public Status getCurrentStatus() {
        return currentStatus;
    }
    /**
     * Reads the next line of text from this file.  This method successively
     * reads bytes from the file, starting at the current file pointer,
     * until it reaches a line terminator or the end
     * of the file.  Each byte is converted into a character by taking the
     * byte's value for the lower eight bits of the character and setting the
     * high eight bits of the character to zero.  This method does not,
     * therefore, support the full Unicode character set.
     *
     * <p> A line of text is terminated by a carriage-return character
     * ({@code '\u005Cr'}), a newline character ({@code '\u005Cn'}), a
     * carriage-return character immediately followed by a newline character,
     * or the end of the file.  Line-terminating characters are discarded and
     * are not included as part of the string returned.
     *
     * <p> This method blocks until a newline character is read, a carriage
     * return and the byte following it are read (to see if it is a newline),
     * the end of the file is reached, or an exception is thrown.
     *
     * @return     the next line of text from this file, or null if end
     *             of file is encountered before even one byte is read.
     * @exception  IOException  if an I/O error occurs.
     */

    public final Line readLine(long startPointer) throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = raf.read()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = raf.getFilePointer();
                    if ((raf.read()) != '\n') {
                        raf.seek(cur);
                    }
                    break;
                default:
                    input.append((char)c);
                    break;
            }
        }

        if ((c == -1) ) {
            if(input.length() == 0)
                return new Line(null,true,true);
            else{
//                if(fileConfig.isCloseEOF())
				if(fileInfo.isCloseEOF())
                    return new Line(input.toString(),true,false);
                else{ // 需要结束本次采集
                    raf.seek(startPointer);
                    return new Line(null,true,false);
                }
            }
        }
        else
            return new Line(input.toString(),false,eol);
    }

    private boolean reachEOFClosed(Line line){
//        if(fileConfig.isCloseEOF() && line.isEof()){
		if(fileInfo.isCloseEOF() && line.isEof()){
            return true;
        }
        return false;
    }

	/**
	 * 单位：毫秒
	 * 从文件采集（fetch）一个batch的数据后，休息一会，避免cpu占用过高，在大量文件同时采集时可以设置，大于0有效，默认值0
	 */
	protected long sleepAwaitTimeAfterFetch = 0L;
	/**
	 * 单位：毫秒
	 * 从文件采集完成一个任务后，休息一会，避免cpu占用过高，在大量文件同时采集时可以设置，大于0有效，默认值0
	 */
	protected long sleepAwaitTimeAfterCollect = 0L;
    protected void execute() {
        boolean reachEOFClosed = false;
        File file = fileInfo.getFile();
        DataTranPlugin dataTranPlugin = fileListenerService.getBaseDataTranPlugin();
        InputPlugin inputPlugin = dataTranPlugin.getInputPlugin();
        try {
            if(taskEnded || inputPlugin.isStopCollectData())
                return;

               String charsetEncode = fileInfo.getCharsetEncode();
//            synchronized (this){  单线程处理，无需同步处理
                if(raf == null) {
                    RandomAccessFile raf = new RandomAccessFile(file, "r");
                    //文件重新写了，则需要重新读取
                    if(pointer > raf.length()){
                        pointer = 0;
                        this.currentStatus.setLastValue(0L);
                    }
                    raf.seek(pointer);
                    this.raf = raf;
                }
                //缓存
                StringBuilder builder = new StringBuilder();
                Line line_;
                String line = null;
                List<Record> recordList = new ArrayList<Record>();
                //批量处理记录数
                int fetchSize = this.fileListenerService.getImportContext().getFetchSize();
                int skipHeaderLines = this.fileConfig.getSkipHeaderLines();
                int readLines = 0;
                long startPointer = pointer;
                long prePointer = 0;

                boolean firstReader = startPointer == 0;
                boolean firstRow = true;
                while(true){
                    prePointer = raf.getFilePointer();
                    line_ = readLine( startPointer);
                    reachEOFClosed = reachEOFClosed(line_);
                    if(line_.getLine() != null) {
                        line = line_.getLine();
                        if(firstReader && skipHeaderLines > 0 && readLines < skipHeaderLines){
                            pointer = raf.getFilePointer();
                            startPointer =  pointer;

                            readLines ++;
                            if(!reachEOFClosed) {
                                if(inputPlugin.isStopCollectData())
                                    break;
                                continue;
                            }
                            else
                            {
                                break;
                            }
                        }
//                        else{
//                            rowStarted = true;
//                            pointer = raf.getFilePointer();
//                            prePointer = pointer;
//                        }
                        if (charsetEncode != null)
                            line = new String(line.getBytes("ISO-8859-1"), charsetEncode);

                        if (null != pattern) {//多行记录匹配模式
                            Matcher m = pattern.matcher(line);
                            if (m.find() && builder.length() > 0) {//下行记录行开始

                                pointer = raf.getFilePointer();
//                                result(file, pointer, builder.toString(), recordList,reachEOFClosed);
                                //应该使用下行记录开始的位置
                                if(firstRow){
                                    firstRow = false;
                                    result(file, prePointer, builder.toString(), recordList,reachEOFClosed);

                                }
                                else{
                                    result(file, prePointer, builder.toString(), recordList,reachEOFClosed);
                                }

                                startPointer =  prePointer;
                                //分批处理数据
                                if (fetchSize > 0 && ( recordList.size() >= fetchSize)) {
                                    fileDataTran.appendData(new CommonData(recordList));
                                    try {
                                        fetchAwaitSleep();
                                    } catch (InterruptedException e) {
                                        break;
                                    }
                                    recordList = new ArrayList<Record>();
                                }

                                builder.setLength(0);
                                if(inputPlugin.isStopCollectData()){
                                    break;
                                }
                            }

                            if (builder.length() > 0) {
                                builder.append(TranUtil.lineSeparator);
                            }
                            builder.append(line);
                            if(reachEOFClosed){
                                pointer = raf.getFilePointer();
                                result(file,pointer,builder.toString(), recordList,reachEOFClosed);
//                                startPointer =  pointer;

                                builder.setLength(0);
                                break;
                            }
                        } else {
                            pointer = raf.getFilePointer();
                            result(file, pointer, line, recordList,reachEOFClosed);
                            startPointer =  pointer;
                            //分批处理数据
                            if (fetchSize > 0 && recordList.size() >= fetchSize) {
                                fileDataTran.appendData(new CommonData(recordList));
                                try {
                                    fetchAwaitSleep();
                                } catch (InterruptedException e) {
                                    break;
                                }
                                recordList = new ArrayList<Record>();
                            }
                            if(reachEOFClosed || inputPlugin.isStopCollectData())
                                break;

                        }
                    }
                    else{
                        break;
                    }
                }
                if(builder.length() > 0 ){
                    if(!line_.isRollbackPreLine()) {
                        pointer = raf.getFilePointer();
                        result(file, pointer, builder.toString(), recordList, reachEOFClosed);
                    }
                    builder.setLength(0);
                    builder = null;
                }
                if(recordList.size() > 0 ){
                    fileDataTran.appendData(new CommonData(recordList));
                    try {
                        fetchAwaitSleep();
                    } catch (InterruptedException e) {

                    }
                }
                //如果设置了文件结束，及结束作业，则进行相应处理，需迁移到通道结束处进行归档和删除处理
                if(reachEOFClosed){

                    /**
                     * 发送空记录
                     */

                    pointer = raf.getFilePointer();
                    sendReadEOFcloseEvent(pointer);

                    taskEnded();

                }
//            }
        }catch (InterruptedException e){
//            logger.error("",e);
        }
        catch (Exception e){
            throw new DataImportException("",e);
        }
        finally {
            destroy();

        }
    }
    protected void sendReadEOFcloseEvent(long pointer) throws IOException, InterruptedException {
        List<Record> recordList = new ArrayList<Record>(1);

        recordList.add(new FileLogRecord(taskContext,true,pointer,true,true));
        fileDataTran.appendData(new CommonData(recordList));
    }

    protected void backupFile(String relativeParentDir,File file) throws IOException {
        String sourcePath = fileConfig.getSourcePath();
        if(fileConfig.isScanChild()) {
            //需要备份到子目录
            String parentDir = SimpleStringUtil.getPath( fileImportConfig.getBackupSuccessFileDir(),relativeParentDir);
            String path = file.getCanonicalPath();
            String toPath = SimpleStringUtil.getPath(parentDir, file.getName());
            FileUtil.bakFile(path, toPath);
            if (logger.isInfoEnabled())
                logger.info("Backup File {} to {}.", path,toPath);
        }
        else{
            String path = file.getCanonicalPath();
            String toPath = SimpleStringUtil.getPath(fileImportConfig.getBackupSuccessFileDir(), file.getName());
            FileUtil.bakFile(path, toPath);
            if (logger.isInfoEnabled())
                logger.info("Backup File {} to {}.", path,toPath);
        }
    }

    public void destroy(){
        if(raf != null){
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            raf = null;
        }
    }

    public void destroyTaskContext(){
        taskContext = null;
    }

    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean regexCheck(String line,Pattern[] patterns,LineMatchType matchType){
//        Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
//        Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
            boolean find = false;
            for(Pattern inc:patterns){
                find = matchType == LineMatchType.REGEX_CONTAIN?inc.matcher(line).find():
                                                                inc.matcher(line).matches();
                if(find)
                    break;
            }
            return find;

    }

    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean stringCheck(String line,String[] patterns,LineMatchType matchType){
//        Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
//        Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
        boolean find = false;
        for(String inc:patterns){
            switch (matchType){
                case STRING_CONTAIN:
                    find = line.contains(inc);
                    break;
                case STRING_EQUALS:
                    find = line.equals(inc);
                    break;
                case STRING_END:
                    find = line.endsWith(inc);
                    break;
                case STRING_PREFIX:
                    find = line.startsWith(inc);
                    break;
                default:
                    break;
            }
            if(find)
                break;
        }
        return find;

    }

    private boolean includeCheck(String line,String[] includeLines,LineMatchType includeLineMatchType){
        boolean find = false;
        switch (includeLineMatchType){
            case REGEX_CONTAIN:
            case REGEX_MATCH:
                Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
                find = regexCheck(line,includes,includeLineMatchType);
                break;
            case STRING_CONTAIN:
            case STRING_EQUALS:
            case STRING_END:
            case STRING_PREFIX:
                find = stringCheck(line,fileConfig.getIncludeLines(),includeLineMatchType);
                break;
            default:
                break;
        }
        return find;
    }

    private boolean excludeCheck(String line,String[] excludeLines,LineMatchType excludeLineMatchType){
        boolean find = false;
        if(excludeLines != null && excludeLines.length > 0){
            switch (excludeLineMatchType){
                case REGEX_CONTAIN:
                case REGEX_MATCH:
                    Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
                    if(regexCheck(line,excludes,excludeLineMatchType)){
                        find = true;
                    }
                    break;
                case STRING_CONTAIN:
                case STRING_EQUALS:
                case STRING_END:
                case STRING_PREFIX:
                    if(stringCheck(line,excludeLines,excludeLineMatchType))
                        find = true;
                    break;
                default:
                    break;
            }
        }
        return find;
    }
    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean check(String line){
        String[] includeLines = fileConfig.getIncludeLines();
        String[] excludeLines = fileConfig.getExcludeLines();
        LineMatchType includeLineMatchType = fileConfig.getIncludeLineMatchType();
        LineMatchType excludeLineMatchType = fileConfig.getExcludeLineMatchType();
        if(includeLines != null && includeLines.length > 0){
            boolean find = includeCheck(line,includeLines, includeLineMatchType);
            if(find){
                if(excludeCheck( line, excludeLines, excludeLineMatchType))
                    find = false;

            }
            return find;
        }
        else{
            boolean find = true;
            if(excludeCheck( line, excludeLines, excludeLineMatchType))
                find = false;
            return find;
        }

    }

    /**
     * 检查记录内容长度是否超过最大长度，如果超过就需要截取掉超过的内容
     * @param line
     * @return
     */
    private String checkMaxLength(String line){
        int maxLength = fileConfig.getMaxBytes();
        if(maxLength > 0 && line.length() > maxLength){
            line = line.substring(0,maxLength);
        }
        return line;
    }
    private boolean needNotSplit(){
    	return SimpleStringUtil.isEmpty(fileConfig.getFieldSplit()) || SimpleStringUtil.isEmpty(fileConfig.getCellMappingList());
	}

    private void result(File file, long pointer, String line,List<Record> recordList,boolean reachEOFClosed) {
        if(!check(line)){
            recordList.add(new FileLogRecord(taskContext,true,pointer,reachEOFClosed,false));
        }
        else {

            Map result = new HashMap();
            try {
                if (jsondata) {
                    //json
                    Map json = SimpleStringUtil.json2Object(line, Map.class);
                    Map addFields = this.getAddFields();
                    if(addFields != null && addFields.size() > 0){
                        json.putAll(addFields);
                    }
                    Map<String, Object> ignoreFields = getIgnoreFields();
                    if(ignoreFields != null && ignoreFields.size() > 0){
                        Iterator iterator = ignoreFields.keySet().iterator();
                        while (iterator.hasNext()){
                            json.remove(iterator.next());
                        }
                    }
                    //同级
                    if (rootLevel) {

                        result = json;
                    } else {//不同级
                        result.put("json", json);
                    }
                } else {

                    if(needNotSplit()) {
						line = checkMaxLength(line);
						result.put("@message", line);

					}
                    else {
                        FieldMappingUtil.buildRecord(result, line,fileConfig.getCellMappingList(),fileConfig.getFieldSplit());
					}
					Map addFields = getAddFields();
					if (addFields != null && addFields.size() > 0) {
						result.putAll(addFields);
					}
                }

            } catch (Exception e) {
                // not json
                result.put("@message", line);
                Map addFields = getAddFields();
                if(addFields != null && addFields.size() > 0){
                    result.putAll(addFields);
                }
            }
            Map common = common(file, pointer, result);
            if (enableMeta) {
                result.put("@filemeta", common);
                result.put("@timestamp",new Date());
            }
            recordList.add(new FileLogRecord(taskContext,common,result,pointer,reachEOFClosed));
        }

    }

    //公共数据
    protected Map common(File file, long pointer, Map result) {
        Map common = new HashMap();
        common.put("hostIp", BaseSimpleStringUtil.getIp());
        common.put("hostName",BaseSimpleStringUtil.getHostName());
        common.put("filePath",FileInodeHandler.change(file.getAbsolutePath()));

        common.put("pointer",pointer);
        common.put("fileId",fileInfo.getFileId());
        FtpConfig ftpConfig = this.fileConfig.getFtpConfig();
        if(ftpConfig != null){
            common.put("ftpDir",ftpConfig.getRemoteFileDir());
            common.put("ftpIp",ftpConfig.getFtpIP());
            common.put("ftpPort",ftpConfig.getFtpPort());
            common.put("ftpUser",ftpConfig.getFtpUser() != null?ftpConfig.getFtpUser():"-");
            common.put("ftpProtocol",ftpConfig.getTransferProtocolName());

        }
        return common;
    }

    public String getFileId() {
        return fileInfo.getFileId();
    }

//    private ReentrantLock changeFileLock = new ReentrantLock();
    public void changeFile(String filePath,File file){
        try {
//            changeFileLock.lock();
            fileInfo.setFile(file);
            fileInfo.setFilePath(filePath);
            if (currentStatus != null) {
//            currentStatus.setFilePath(FileInodeHandler.change(file.getAbsolutePath()));
                currentStatus.setRealPath(filePath);//设置实际文件地址，原文件地址不变
            }
        }
        finally {
//            changeFileLock.unlock();
        }
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "{\"file\":\""+ FileInodeHandler.change(fileInfo.getFile().getAbsolutePath())+"\",\"fileId\":\""
                +fileInfo.getFileId()+"\",\"pointer\":"+pointer+"}";
    }

    public boolean isTaskEnded() {
        return taskEnded;
    }

    public void taskEnded() {
        if(taskEnded)
            return;
        synchronized (this) {
            if(taskEnded)
                return;
            this.taskEnded = true;
//            this.currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
            this.fileDataTran.stop();

        }
    }
    public BaseDataTran getFileDataTran() {
        return fileDataTran;
    }
    protected void fetchAwaitSleep() throws InterruptedException{
        if(sleepAwaitTimeAfterFetch > 0L) {
            try {
                sleep(sleepAwaitTimeAfterFetch);
            } catch (InterruptedException e) {
                throw e;
            }
        }
    }
}
