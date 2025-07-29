package com.kummyong.nifi.ftp;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.IOException;
import java.io.InputStream;

public class CommonsNetFtpClient implements FtpClient {
    private FTPClient client;

    @Override
    public void connect(String host, int port, String username, String password,
                        int connectTimeout, int dataTimeout, boolean passiveMode) throws IOException {
        client = new FTPClient();
        client.setConnectTimeout(connectTimeout);
        client.setSoTimeout(dataTimeout);
        client.setDataTimeout(dataTimeout);
        client.connect(host, port);
        client.login(username, password);
        client.setFileType(FTP.BINARY_FILE_TYPE);
        if (passiveMode) {
            client.enterLocalPassiveMode();
        } else {
            client.enterLocalActiveMode();
        }
    }

    @Override
    public InputStream retrieveFile(String remotePath) throws IOException {
        return client.retrieveFileStream(remotePath);
    }

    @Override
    public void completePendingCommand() throws IOException {
        if (client != null) {
            client.completePendingCommand();
        }
    }

    @Override
    public void disconnect() throws IOException {
        if (client != null && client.isConnected()) {
            try {
                client.logout();
            } finally {
                client.disconnect();
            }
        }
    }
}
