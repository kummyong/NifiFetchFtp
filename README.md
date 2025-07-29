# NifiFetchFtp

A prototype NiFi processor for fetching files from an FTP server. This module allows selecting between different FTP client libraries via processor configuration.

The sample implementation provides two client options:

- **Apache Commons Net** – uses the widely used `FTPClient` implementation.
- **URLConnection** – uses the standard JDK URL connection to retrieve files.

Additional processor properties allow configuring connection timeout, data timeout
and whether to use passive mode. These settings are helpful when troubleshooting
issues like socket timeout during file download.

The Commons Net implementation now completes pending FTP commands after each
file transfer to avoid lingering connections that could lead to timeouts.

The processor is built with Maven and packaged as a NiFi NAR.
