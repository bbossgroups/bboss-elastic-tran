package org.frameworkset.tran.jobflow.scan;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.nosql.s3.OSSClient;
import org.frameworkset.nosql.s3.OSSFile;
import org.frameworkset.tran.input.file.*;
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.input.s3.OSSFilterFileInfo;
import org.frameworkset.tran.jobflow.DownloadfileConfig;
import org.frameworkset.tran.jobflow.FileDownloadService;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 扫描ftp服务器上面指定目录下面新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public class JobS3DirScan extends JobLogDirScan {
    private Logger logger = LoggerFactory.getLogger(JobS3DirScan.class);
    private OSSClient ossClient ;
    private OSSFileInputConfig ossFileConfig;
 
    public JobS3DirScan(DownloadfileConfig downloadfileConfig, FileDownloadService fileDownloadService) {
        super( downloadfileConfig, fileDownloadService);
       
        this.ossFileConfig = downloadfileConfig.getOssFileInputConfig();
        this.ossClient = ossFileConfig.getOssClient();
    }

    /**
     * 工作流作业节点调用：识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        if(logger.isDebugEnabled()){
            if(downloadfileConfig.getJobFileFilter() == null)
                logger.debug("Scan new oss file in remote dir {} with filename regex {}.",ossFileConfig.getRemoteFileDir(),downloadfileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new oss file in remote dir {} with filename filter {}.",ossFileConfig.getRemoteFileDir(),
                        downloadfileConfig.getJobFileFilter().getClass().getCanonicalName());
            }
        }
        JobFileFilter fileFilter = downloadfileConfig.getJobFileFilter();
        List<OSSFile> files = ossClient.listOssFile(ossFileConfig.getBucket(),ossFileConfig.getRemoteFileDir(), downloadfileConfig.isScanChild());
        files = filterFiles(  fileFilter,  files,  jobFlowNodeExecuteContext);
        
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.warn("Remote oss dir[{}] is a file or empty directory.", ossFileConfig.getRemoteFileDir());
            }
            return;
        }
        List<Future> downloadFutures = ossFileConfig.getRemoteFileChannel() != null?new ArrayList<>():null;
        try {
            _scanCheck(jobFlowNodeExecuteContext,"", files, downloadFutures);
        }
        finally {
            if(downloadFutures != null && downloadFutures.size() > 0){
                for(Future future:downloadFutures){
                    try {
                        future.get();
                    } catch (InterruptedException e) {

                    } catch (ExecutionException e) {

                    }
                }
            }
        }
    }

    private List<OSSFile> filterFiles(JobFileFilter fileFilter,List<OSSFile> files,JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        if(fileFilter != null){
            List<OSSFile> newFiles = new ArrayList<>();
            for(int i = 0; i < files.size(); i ++ ){
                OSSFile file = files.get(i);
                FilterFileInfo fileInfo = new OSSFilterFileInfo(file);
                if(fileFilter.accept(fileInfo,jobFlowNodeExecuteContext)){
                    newFiles.add(file);
                }
            }
            return newFiles;
        }
        else{
            return files;
        }
      
    }
    
    
    public void scanSubDirNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,String relativeParentDir,OSSFile logDir,List<Future> downloadFutures){
        String path = SimpleStringUtil.getPath(relativeParentDir,logDir.getObjectName());
        if(logger.isDebugEnabled()){
            if(downloadfileConfig.getJobFileFilter() == null)
                logger.debug("Scan new oss file in remote dir {} with filename regex {}.",path,downloadfileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new oss file in remote dir {} with filename filter {}.",path,
                        downloadfileConfig.getJobFileFilter().getClass().getCanonicalName());
            }
        }
        JobFileFilter fileFilter = downloadfileConfig.getJobFileFilter();
        List<OSSFile> files = ossClient.listOssFile(ossFileConfig.getBucket(),null);
        files = filterFiles(  fileFilter,  files,  jobFlowNodeExecuteContext);
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.info("{} must be a directory or is empty directory.",path);
            }
            return;
        }
        _scanCheck(  jobFlowNodeExecuteContext,relativeParentDir,files, downloadFutures);
    }
 
    /**
     *
     * @param relativeParentDir 相对路径
     * @param files
     */
    private void _scanCheck(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,String relativeParentDir,List<OSSFile> files,List<Future> downloadFutures){
        for(OSSFile remoteResourceInfo:files){
            if (jobFlowNodeExecuteContext.assertStopped().isTrue()) {
                break;
            }
            if(remoteResourceInfo.isDir()) {
                String path = SimpleStringUtil.getPath(relativeParentDir,remoteResourceInfo.getObjectName());
                if(!downloadfileConfig.isScanChild()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Ignore oss dir:{}", path);
                    }
                    continue;
                }
                else{
                    scanSubDirNewFile(    jobFlowNodeExecuteContext,path,remoteResourceInfo, downloadFutures);
                }
            }
            else{
                fileDownloadService.checkOSSNewFile(  jobFlowNodeExecuteContext,relativeParentDir,remoteResourceInfo,this.ossFileConfig,downloadFutures,ossClient);
            }
        }
    }
    


}
