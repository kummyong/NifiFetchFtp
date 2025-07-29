package com.kummyong.nifi.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class UrlFtpClient implements FtpClient {
    private String host;
    private int port;
    private String username;
    private String password;
    private int connectTimeout;
    private int readTimeout;

    @Override
    public void connect(String host, int port, String username, String password,
                        int connectTimeout, int dataTimeout, boolean passiveMode) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.connectTimeout = connectTimeout;
        this.readTimeout = dataTimeout;
    }

    @Override
    public InputStream retrieveFile(String remotePath) throws IOException {
        String url = String.format("ftp://%s:%s@%s:%d/%s;type=i", username, password, host, port, remotePath);
        URLConnection conn = new URL(url).openConnection();
        conn.setConnectTimeout(connectTimeout);
        conn.setReadTimeout(readTimeout);
        return conn.getInputStream();
    }

    @Override
    public void completePendingCommand() {
        // no-op for URL connection
    }

    @Override
    public void disconnect() {
        // Nothing to close for URL connections
    }
}
