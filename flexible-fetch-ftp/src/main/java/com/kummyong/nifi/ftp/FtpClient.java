package com.kummyong.nifi.ftp;

import java.io.IOException;
import java.io.InputStream;

public interface FtpClient {
    void connect(String host, int port, String username, String password,
                 int connectTimeout, int dataTimeout, boolean passiveMode) throws IOException;
    InputStream retrieveFile(String remotePath) throws IOException;
    void completePendingCommand() throws IOException;
    void disconnect() throws IOException;
}
