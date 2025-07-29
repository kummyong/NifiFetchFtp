package com.kummyong.nifi.ftp;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"ftp", "fetch", "prototype"})
@CapabilityDescription("Fetches a file from an FTP server using a selectable FTP library")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class FlexibleFetchFTP extends AbstractProcessor {

    public static final AllowableValue APACHE_CLIENT = new AllowableValue(
            "apache", "Apache Commons Net", "Use Apache Commons Net FTPClient");
    public static final AllowableValue URL_CLIENT = new AllowableValue(
            "url", "URLConnection", "Use basic URLConnection based client");

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("FTP Host")
            .description("FTP server hostname")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("FTP Port")
            .description("FTP server port")
            .required(true)
            .defaultValue("21")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("FTP Username")
            .description("Username")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("FTP Password")
            .description("Password")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_FILE = new PropertyDescriptor.Builder()
            .name("Remote File")
            .description("Remote file path to fetch")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_TYPE = new PropertyDescriptor.Builder()
            .name("FTP Client Type")
            .description("Which FTP client implementation to use")
            .required(true)
            .allowableValues(APACHE_CLIENT, URL_CLIENT)
            .defaultValue(APACHE_CLIENT.getValue())
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("FTP connection timeout in milliseconds")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DATA_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Data Timeout")
            .description("FTP data timeout in milliseconds")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSIVE_MODE = new PropertyDescriptor.Builder()
            .name("Passive Mode")
            .description("Use passive mode for data connections")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully fetched files")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be fetched")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final org.apache.nifi.processor.ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOST);
        descriptors.add(PORT);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(REMOTE_FILE);
        descriptors.add(CLIENT_TYPE);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(DATA_TIMEOUT);
        descriptors.add(PASSIVE_MODE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    protected FtpClient buildClient(String type) {
        if (URL_CLIENT.getValue().equals(type)) {
            return new UrlFtpClient();
        }
        return new CommonsNetFtpClient();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String host = context.getProperty(HOST).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        String remote = context.getProperty(REMOTE_FILE).evaluateAttributeExpressions(flowFile).getValue();
        String type = context.getProperty(CLIENT_TYPE).getValue();
        int connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asInteger();
        int dataTimeout = context.getProperty(DATA_TIMEOUT).evaluateAttributeExpressions().asInteger();
        boolean passive = context.getProperty(PASSIVE_MODE).asBoolean();

        FtpClient client = buildClient(type);
        try {
            client.connect(host, port, username, password, connectTimeout, dataTimeout, passive);
            try (InputStream in = client.retrieveFile(remote)) {
                if (in == null) {
                    getLogger().error("Could not retrieve file {}", new Object[]{remote});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
                flowFile = session.importFrom(in, flowFile);
                client.completePendingCommand();
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException e) {
            getLogger().error("Failed to fetch file", e);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            try {
                client.disconnect();
            } catch (IOException ignore) {
            }
        }
    }
}
