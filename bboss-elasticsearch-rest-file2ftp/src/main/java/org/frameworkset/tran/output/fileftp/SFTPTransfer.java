package org.frameworkset.tran.output.fileftp;
/**
 * Copyright 2020 bboss
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

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.userauth.UserAuthException;
import net.schmizz.sshj.xfer.FileSystemFile;
import org.frameworkset.tran.DataImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 20:27
 * @author biaoping.yin
 * @version 1.0
 */
public class SFTPTransfer {
	private static Logger logger = LoggerFactory.getLogger(SFTPTransfer.class);
	public static synchronized void sendFile(FileFtpOupputContext fileFtpOupputContext, String filePath){
		final SSHClient ssh = new SSHClient();

		try {
			ssh.loadKnownHosts();
			if(fileFtpOupputContext.getHostKeyVerifiers() != null) {
				for(String hostKey:fileFtpOupputContext.getHostKeyVerifiers())
					ssh.addHostKeyVerifier(hostKey);
			}
			ssh.connect(fileFtpOupputContext.getFtpIP(),fileFtpOupputContext.getFtpPort());
			if(fileFtpOupputContext.getKeepAliveTimeout() > 0 )
				ssh.getConnection().getKeepAlive().setKeepAliveInterval((int)(fileFtpOupputContext.getKeepAliveTimeout() / 1000)); //every 60sec
//            ssh.addHostKeyVerifier(new PromiscuousVerifier());
			ssh.authPassword(fileFtpOupputContext.getFtpUser(),fileFtpOupputContext.getFtpPassword());
			final SFTPClient sftp = ssh.newSFTPClient();
			try {
				sftp.put(new FileSystemFile(filePath), fileFtpOupputContext.getRemoteFileDir());
				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]");
			} finally {
				sftp.close();
			}
		} catch (UserAuthException e) {
			logger.error("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]",e);
			throw new DataImportException("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort()
					+ " failed:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]",e);
		} catch (TransportException e) {
			logger.error("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]",e);
			throw new DataImportException("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort()
					+ " Send:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]",e);
		} catch (IOException e) {
			logger.error("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]",e);
			throw new DataImportException("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort()
					+ " failed:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]",e);
		} finally {
			try {
				ssh.disconnect();
			} catch (IOException e) {
				logger.warn("ssh " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " disconnect failed:",e);
			}
		}
	}

}
