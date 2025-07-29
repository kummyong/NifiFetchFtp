package com.kummyong.nifi.ftp;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class FlexibleFetchFTPTest {

    static class MockFtpClient implements FtpClient {
        boolean connected;
        boolean completed;
        boolean disconnected;
        InputStream stream;
        @Override
        public void connect(String host, int port, String username, String password,
                             int connectTimeout, int dataTimeout, boolean passiveMode) {
            connected = true;
        }
        @Override
        public InputStream retrieveFile(String remotePath) {
            return stream;
        }
        @Override
        public void completePendingCommand() {
            completed = true;
        }
        @Override
        public void disconnect() {
            disconnected = true;
        }
    }

    static class TestProcessor extends FlexibleFetchFTP {
        private final FtpClient client;
        TestProcessor(FtpClient client) {
            this.client = client;
        }
        @Override
        protected FtpClient buildClient(String type) {
            return client;
        }
    }

    private MockFtpClient client;
    private TestRunner runner;

    @BeforeEach
    void setup() {
        client = new MockFtpClient();
        runner = TestRunners.newTestRunner(new TestProcessor(client));
        runner.setProperty(FlexibleFetchFTP.HOST, "localhost");
        runner.setProperty(FlexibleFetchFTP.PORT, "21");
        runner.setProperty(FlexibleFetchFTP.USERNAME, "user");
        runner.setProperty(FlexibleFetchFTP.PASSWORD, "pass");
        runner.setProperty(FlexibleFetchFTP.REMOTE_FILE, "${filename}");
        runner.setProperty(FlexibleFetchFTP.CONNECT_TIMEOUT, "1000");
        runner.setProperty(FlexibleFetchFTP.DATA_TIMEOUT, "1000");
        runner.setProperty(FlexibleFetchFTP.PASSIVE_MODE, "true");
    }

    @Test
    void testSuccessfulFetch() throws IOException {
        client.stream = new ByteArrayInputStream("data".getBytes());
        runner.enqueue(new byte[0], Collections.singletonMap("filename", "remote.txt"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FlexibleFetchFTP.REL_SUCCESS, 1);
        assertTrue(client.connected);
        assertTrue(client.completed);
        assertTrue(client.disconnected);
    }

    @Test
    void testMissingFile() throws IOException {
        client.stream = null;
        runner.enqueue(new byte[0], Collections.singletonMap("filename", "remote.txt"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FlexibleFetchFTP.REL_FAILURE, 1);
        assertTrue(client.connected);
        assertTrue(client.disconnected);
    }
}
