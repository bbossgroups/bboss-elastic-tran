package org.frameworkset.tran.input.file;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.OSInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class FileImportConfig extends BaseImportConfig {
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
    private long checkFileModifyInterval = 3000l;

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
    public FileImportConfig() {
    }




    public List<FileConfig> getFileConfigList() {
        return fileConfigList;
    }
    public FileImportConfig addConfig(FileConfig fileConfig){

        if(fileConfig.getFtpConfig() != null || fileConfig.isScanChild() || OSInfo.isWindows()){
            fileConfig.setEnableInode(false);//ftp需禁用inode机制
        }

        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }

        fileConfig.init();
        fileConfigList.add(fileConfig);

        return this;
    }
    public FileImportConfig addConfig(String sourcePath,String fileNameRegular,String fileHeadLine){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(new FileConfig(sourcePath,fileNameRegular,fileHeadLine).init());
        return this;
    }
    public FileImportConfig addConfig(String sourcePath,String fileNameRegular,String fileHeadLine,boolean scanChild){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(new FileConfig(sourcePath,fileNameRegular,fileHeadLine,scanChild).init());
        return this;
    }

    /**
     *
     * @param interval
     * use method setScanNewFileInterval
     * @return
     */
    @Deprecated
    public FileImportConfig setInterval(Long interval) {
        return setScanNewFileInterval(interval);
    }
    public FileImportConfig setScanNewFileInterval(Long scanNewFileInterval) {
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

    public FileImportConfig setRootLevel(boolean rootLevel) {
        this.rootLevel = rootLevel;
        return this;
    }

    public FileImportConfig setJsondata(boolean jsondata) {
        this.jsondata = jsondata;
        return this;
    }

    public String getCharsetEncode() {
        return charsetEncode;
    }

    public FileImportConfig setCharsetEncode(String charsetEncode) {
        this.charsetEncode = charsetEncode;
        return this;
    }

    public boolean isEnableMeta() {
        return enableMeta;
    }

    public FileImportConfig setEnableMeta(boolean enableMeta) {
        this.enableMeta = enableMeta;
        return this;
    }

    public Long getRegistLiveTime() {
        return registLiveTime;
    }

    public FileImportConfig setRegistLiveTime(Long registLiveTime) {
        this.registLiveTime = registLiveTime;
        return this;
    }


    public long getCheckFileModifyInterval() {
        return checkFileModifyInterval;
    }

    public FileImportConfig setCheckFileModifyInterval(long checkFileModifyInterval) {
        this.checkFileModifyInterval = checkFileModifyInterval;
        return this;
    }
    public boolean isBackupSuccessFiles() {
        return backupSuccessFiles;
    }

    public FileImportConfig setBackupSuccessFiles(boolean backupSuccessFiles) {
        this.backupSuccessFiles = backupSuccessFiles;
        return this;
    }

    public String getBackupSuccessFileDir() {
        return backupSuccessFileDir;
    }

    public FileImportConfig setBackupSuccessFileDir(String backupSuccessFileDir) {
        this.backupSuccessFileDir = backupSuccessFileDir;
        return this;
    }

    public long getBackupSuccessFileInterval() {
        return backupSuccessFileInterval;
    }

    public FileImportConfig setBackupSuccessFileInterval(long backupSuccessFileInterval) {
        this.backupSuccessFileInterval = backupSuccessFileInterval;
        return this;
    }

    public long getBackupSuccessFileLiveTime() {
        return backupSuccessFileLiveTime;
    }

    public FileImportConfig setBackupSuccessFileLiveTime(long backupSuccessFileLiveTime) {
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
    public FileImportConfig setUseETLScheduleForScanNewFile(boolean useETLScheduleForScanNewFile) {
        this.useETLScheduleForScanNewFile = useETLScheduleForScanNewFile;
        return this;
    }

    public FileReaderTask buildFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
                                                 Status currentStatus , FileImportConfig fileImportConfig ){
        FileReaderTask task = new FileReaderTask(taskContext,file,fileId,fileConfig,pointer,
                fileListenerService,fileDataTran,currentStatus,fileImportConfig);
        return task;
    }
    public FileReaderTask buildFileReaderTask(String fileId,  Status currentStatus,FileImportConfig fileImportConfig ){
        FileReaderTask task =  new FileReaderTask(fileId,currentStatus,fileImportConfig);
        return task;
    }
}
