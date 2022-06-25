package org.frameworkset.tran.plugin.file.input;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.schedule.timer.TimeRange;
import org.frameworkset.tran.schedule.timer.TimerScheduleConfig;
import org.frameworkset.util.OSInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class FileInputConfig extends BaseConfig implements InputConfig {
    /**
     * 每次扫描新文件时间间隔
     */

    private Long scanNewFileInterval = 5000L;
    private Long registLiveTime;
    //jsondata = true时，自定义的数据是否和采集的数据平级，true则直接在原先的json串中存放数据
    //false则定义一个json存放数据，若不是json则是message
    private boolean rootLevel = true;
    /**
     * jsondata：标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
     */
    private boolean jsondata ;
    private boolean enableMeta;
    private String charsetEncode = "UTF-8";
    private List<FileConfig> fileConfigList;
    private long checkFileModifyInterval = 2000l;
    /**
     * 单位：毫秒
     * 从文件采集（fetch）一个batch的数据后，休息一会，避免cpu占用过高，在大量文件同时采集时可以设置，大于0有效，默认值0
     */
    protected long sleepAwaitTimeAfterFetch = 0l;
    /**
     * 单位：毫秒
     * 从文件采集完成一个任务后，休息一会，避免cpu占用过高，在大量文件同时采集时可以设置，大于0有效，默认值0
     */
    protected long sleepAwaitTimeAfterCollect = 0l;
    public TimerScheduleConfig getTimerScheduleConfig() {
        return timerScheduleConfig;
    }

    private TimerScheduleConfig timerScheduleConfig;
    /**
     * 备份成功文件
     * true 备份
     * false 不备份
     */
    private boolean backupSuccessFiles;
    /**
     * 备份文件目录
     */
    private String backupSuccessFileDir;

    /**
     * 备份文件清理线程执行时间间隔，单位：毫秒
     * 默认每隔10秒执行一次
     */
    private long backupSuccessFileInterval = 10000l;
    /**
     * 备份文件保留时长，单位：秒
     * 默认保留7天
     */
    private long backupSuccessFileLiveTime = 7 * 24 * 60 * 60l;
    /**
     * 否采用外部新文件扫描调度机制：jdk timer,quartz,xxl-job
     *     true 采用，false 不采用，默认false
     */
    private boolean useETLScheduleForScanNewFile;
    public FileInputConfig() {
    }




    public List<FileConfig> getFileConfigList() {
        return fileConfigList;
    }
    private boolean enableAutoPauseScheduled = true;

    public boolean isEnableAutoPauseScheduled() {
        return enableAutoPauseScheduled;
    }

    public FileInputConfig addConfig(FileConfig fileConfig){



        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        if(fileConfig.getFtpConfig() != null || fileConfig.isScanChild() || OSInfo.isWindows()){
            fileConfig.setEnableInode(false);//ftp需禁用inode机制
        }
        if(!fileConfig.isCloseEOF()){
            enableAutoPauseScheduled = false;
        }

//        fileConfig.init();
        fileConfigList.add(fileConfig);

        return this;
    }
    public FileInputConfig addConfig(String sourcePath, String fileNameRegular, String fileHeadLine){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        FileConfig fileConfig = new FileConfig(sourcePath,fileNameRegular,fileHeadLine);
        if(OSInfo.isWindows()){
            fileConfig.setEnableInode(false);//ftp需禁用inode机制
        }
        if(!fileConfig.isCloseEOF()){
            enableAutoPauseScheduled = false;
        }
        fileConfigList.add(fileConfig);
        return this;
    }
    public FileInputConfig addConfig(String sourcePath, String fileNameRegular, String fileHeadLine, boolean scanChild){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        FileConfig fileConfig = new FileConfig(sourcePath,fileNameRegular,fileHeadLine,scanChild);
        if(scanChild || OSInfo.isWindows()){
            fileConfig.setEnableInode(false);//ftp需禁用inode机制
        }
        if(!fileConfig.isCloseEOF()){
            enableAutoPauseScheduled = false;
        }
        fileConfigList.add(fileConfig);
        return this;
    }

    /**
     *
     * @param interval
     * use method setScanNewFileInterval
     * @return
     */
    @Deprecated
    public FileInputConfig setInterval(Long interval) {
        return setScanNewFileInterval(interval);
    }
    public FileInputConfig setScanNewFileInterval(Long scanNewFileInterval) {
        this.scanNewFileInterval = scanNewFileInterval;
        return this;
    }
    public Long getScanNewFileInterval() {
        return scanNewFileInterval;
    }

    public boolean isRootLevel() {
        return rootLevel;
    }

    public boolean isJsondata() {
        return jsondata;
    }

    public FileInputConfig setRootLevel(boolean rootLevel) {
        this.rootLevel = rootLevel;
        return this;
    }

    public FileInputConfig setJsondata(boolean jsondata) {
        this.jsondata = jsondata;
        return this;
    }

    public String getCharsetEncode() {
        return charsetEncode;
    }

    public FileInputConfig setCharsetEncode(String charsetEncode) {
        this.charsetEncode = charsetEncode;
        return this;
    }

    public boolean isEnableMeta() {
        return enableMeta;
    }

    public FileInputConfig setEnableMeta(boolean enableMeta) {
        this.enableMeta = enableMeta;
        return this;
    }

    public Long getRegistLiveTime() {
        return registLiveTime;
    }

    public FileInputConfig setRegistLiveTime(Long registLiveTime) {
        this.registLiveTime = registLiveTime;
        return this;
    }


    public long getCheckFileModifyInterval() {
        return checkFileModifyInterval;
    }

    public FileInputConfig setCheckFileModifyInterval(long checkFileModifyInterval) {
        this.checkFileModifyInterval = checkFileModifyInterval;
        return this;
    }
    public boolean isBackupSuccessFiles() {
        return backupSuccessFiles;
    }

    public FileInputConfig setBackupSuccessFiles(boolean backupSuccessFiles) {
        this.backupSuccessFiles = backupSuccessFiles;
        return this;
    }

    public String getBackupSuccessFileDir() {
        return backupSuccessFileDir;
    }

    public FileInputConfig setBackupSuccessFileDir(String backupSuccessFileDir) {
        this.backupSuccessFileDir = backupSuccessFileDir;
        return this;
    }

    public long getBackupSuccessFileInterval() {
        return backupSuccessFileInterval;
    }

    public FileInputConfig setBackupSuccessFileInterval(long backupSuccessFileInterval) {
        this.backupSuccessFileInterval = backupSuccessFileInterval;
        return this;
    }

    public long getBackupSuccessFileLiveTime() {
        return backupSuccessFileLiveTime;
    }

    public FileInputConfig setBackupSuccessFileLiveTime(long backupSuccessFileLiveTime) {
        this.backupSuccessFileLiveTime = backupSuccessFileLiveTime;
        return this;
    }

    public boolean isUseETLScheduleForScanNewFile() {
        return useETLScheduleForScanNewFile;
    }

    /**
     *  设置是否采用外部新文件扫描调度机制：jdk timer,quartz,xxl-job
     *      true 采用，false 不采用，默认false
     * @param useETLScheduleForScanNewFile
     * @return
     */
    public FileInputConfig setUseETLScheduleForScanNewFile(boolean useETLScheduleForScanNewFile) {
        this.useETLScheduleForScanNewFile = useETLScheduleForScanNewFile;
        return this;
    }

    public FileReaderTask buildFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
											  Status currentStatus , FileInputConfig fileImportConfig ){
        FileReaderTask task = new FileReaderTask(taskContext,file,fileId,fileConfig,pointer,
                fileListenerService,fileDataTran,currentStatus,fileImportConfig);
        return task;
    }
    public FileReaderTask buildFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig ){
        FileReaderTask task =  new FileReaderTask(fileId,currentStatus,fileImportConfig);
        return task;
    }
    /**
     * 添加不扫码新文件的时间段
     * timeRange必须是以下三种类型格式
     * 11:30-12:30  每天在11:30和12:30之间运行
     * 11:30-    每天11:30开始执行,到23:59结束
     * -12:30    每天从00:00开始到12:30
     * @param timeRange
     * @return
     */
    public FileInputConfig addSkipScanNewFileTimeRange(String timeRange){
        if(timerScheduleConfig == null){
            timerScheduleConfig = new TimerScheduleConfig();
        }
        timerScheduleConfig.addSkipScanNewFileTimeRange(timeRange);
        return this;
    }

    /**
     * 添加扫码新文件的时间段，每天扫描新文件时间段，优先级高于不扫码时间段，先计算是否在扫描时间段，如果是则扫描，不是则不扫码
     * timeRange必须是以下三种类型格式
     * 11:30-12:30  每天在11:30和12:30之间运行
     * 11:30-    每天11:30开始执行,到23:59结束
     * -12:30    每天从00:00开始到12:30
     * @param timeRange
     * @return
     */
    public FileInputConfig addScanNewFileTimeRange(String timeRange){
        if(timerScheduleConfig == null){
            timerScheduleConfig = new TimerScheduleConfig();
        }
        timerScheduleConfig.addSkipScanNewFileTimeRange(timeRange);

        return this;
    }



    public List<TimeRange> getScanNewFileTimeRanges() {
        return timerScheduleConfig != null?timerScheduleConfig.getScanNewFileTimeRanges():null;
    }

    public List<TimeRange> getSkipScanNewFileTimeRanges() {
        return timerScheduleConfig != null?timerScheduleConfig.getSkipScanNewFileTimeRanges():null;
    }

    public FileInputConfig setSleepAwaitTimeAfterFetch(long sleepAwaitTimeAfterFetch) {
        this.sleepAwaitTimeAfterFetch = sleepAwaitTimeAfterFetch;
        return this;
    }

    public long getSleepAwaitTimeAfterFetch() {
        return sleepAwaitTimeAfterFetch;
    }

    public FileInputConfig setSleepAwaitTimeAfterCollect(long sleepAwaitTimeAfterCollect) {
        this.sleepAwaitTimeAfterCollect = sleepAwaitTimeAfterCollect;
        return this;
    }

    public long getSleepAwaitTimeAfterCollect() {
        return sleepAwaitTimeAfterCollect;
    }

    @Override
    public void build(ImportBuilder importBuilder) {

    }

    @Override
    public InputPlugin getInputPlugin(ImportContext importContext) {
        FileInputDataTranPlugin fileInputDataTranPlugin = new FileInputDataTranPlugin(importContext);

        return fileInputDataTranPlugin;
    }

    @Override
    public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
        DataTranPlugin dataTranPlugin = new FileDataTranPluginImpl(importContext);
        return dataTranPlugin;
    }
}
