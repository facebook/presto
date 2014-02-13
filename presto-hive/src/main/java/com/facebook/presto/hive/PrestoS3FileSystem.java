/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.Iterators;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.toArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;

public class PrestoS3FileSystem
        extends FileSystem
{
    private static final Logger log = Logger.get(PrestoS3FileSystem.class.getSimpleName());

    private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);
    private static final int DEFAULT_MAX_CLIENT_RETRY = 3;
    private static final int DEFAULT_MAX_ERROR_RETRY = 10;
    private static final int DEFAULT_CONN_TIMEOUT = 100000;

    private URI uri;
    private Path workingDirectory;
    private AmazonS3 s3;
    private Configuration config;
    private int maxErrorRetry;
    private int maxClientRetry;

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        checkNotNull(uri, "uri is null");
        checkNotNull(conf, "conf is null");

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDirectory = new Path("/").makeQualified(this.uri, new Path("/"));
        this.config = conf;
        ClientConfiguration configuration = new ClientConfiguration();
        this.maxErrorRetry = conf.getInt("fs.s3n.max.error.retry", DEFAULT_MAX_ERROR_RETRY);
        this.maxClientRetry = conf.getInt("fs.s3n.max.client.retry", DEFAULT_MAX_CLIENT_RETRY);
        configuration.setMaxErrorRetry(this.maxErrorRetry);
        boolean enableSSL = conf.getBoolean("fs.s3n.ssl.enabled", true);
        if (enableSSL) {
            configuration.setProtocol(Protocol.HTTPS);
        }
        else {
            configuration.setProtocol(Protocol.HTTP);
        }
        configuration.setConnectionTimeout(conf.getInt("fs.s3n.conn.timeout", DEFAULT_CONN_TIMEOUT));
        this.s3 = new AmazonS3Client(getAwsCredentials(uri, config), configuration);
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        workingDirectory = path;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return toArray(list, LocatedFileStatus.class);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path path)
            throws IOException
    {
        return new RemoteIterator<LocatedFileStatus>()
        {
            private final Iterator<LocatedFileStatus> iterator = listPrefix(path);

            @Override
            public boolean hasNext()
                    throws IOException
            {
                try {
                    return iterator.hasNext();
                }
                catch (AmazonClientException e) {
                    throw new IOException(e);
                }
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                try {
                    return iterator.next();
                }
                catch (AmazonClientException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        try {
            ObjectMetadata metadata = getS3ObjectMetadata(path);
            long lastModifiedTime = 0L;
            if (metadata.getLastModified() != null) {
                lastModifiedTime = metadata.getLastModified().getTime();
            }
            return new FileStatus(
               metadata.getContentLength(),
                false,
                1,
                BLOCK_SIZE.toBytes(),
                lastModifiedTime,
               qualifiedPath(path));
        }
        catch (FileNotFoundException e) {
            Iterator<LocatedFileStatus> fileStats = listPrefix(path);
            // it is a directory, not a file
            if (fileStats != null && fileStats.hasNext()) {
                return new FileStatus(
                        0L, true, 1, 0L, 0L, qualifiedPath(path));
            }
            else {
                throw e;
            }
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new PrestoS3InputStream(s3, uri.getHost(), path, maxClientRetry),
                        bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        if ((exists(f)) && (!overwrite)) {
            throw new IOException("File already exists:" + f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = keyFromPath(absolutePath);
        return new FSDataOutputStream(
                new PrestoS3OutputStream(s3, uri.getHost(), config, key, progress, bufferSize),
                this.statistics);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException
    {
        throw new UnsupportedOperationException("append");
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        throw new UnsupportedOperationException("rename");
    }

    @Override
    public boolean delete(Path f, boolean recursive)
            throws IOException
    {
        throw new UnsupportedOperationException("delete");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException
    {
        //no operation
        return true;
    }

    private Iterator<LocatedFileStatus> listPrefix(Path path)
    {
        String key = keyFromPath(path);
        if (!key.isEmpty()) {
            key += "/";
        }

        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(uri.getHost())
                .withPrefix(key)
                .withDelimiter("/");

        Iterator<ObjectListing> listings = new AbstractSequentialIterator<ObjectListing>(s3.listObjects(request))
        {
            @Override
            protected ObjectListing computeNext(ObjectListing previous)
            {
                if (!previous.isTruncated()) {
                    return null;
                }
                return s3.listNextBatchOfObjects(previous);
            }
        };

        return Iterators.concat(Iterators.transform(listings, statusFromListing()));
    }

    private Function<ObjectListing, Iterator<LocatedFileStatus>> statusFromListing()
    {
        return new Function<ObjectListing, Iterator<LocatedFileStatus>>()
        {
            @Override
            public Iterator<LocatedFileStatus> apply(ObjectListing listing)
            {
                return Iterators.concat(
                        statusFromPrefixes(listing.getCommonPrefixes()),
                        statusFromObjects(listing.getObjectSummaries()));
            }
        };
    }

    private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes)
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        for (String prefix : prefixes) {
            Path path = qualifiedPath(new Path("/" + prefix));
            FileStatus status = new FileStatus(0, true, 1, BLOCK_SIZE.toBytes(), 0, path);
            list.add(createLocatedFileStatus(status));
        }
        return list.iterator();
    }

    private Iterator<LocatedFileStatus> statusFromObjects(List<S3ObjectSummary> objects)
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        for (S3ObjectSummary object : objects) {
            if (!object.getKey().endsWith("/")) {
                FileStatus status = new FileStatus(
                        object.getSize(),
                        false,
                        1,
                        BLOCK_SIZE.toBytes(),
                        object.getLastModified().getTime(),
                        qualifiedPath(new Path("/" + object.getKey())));
                list.add(createLocatedFileStatus(status));
            }
        }
        return list.iterator();
    }

    private ObjectMetadata getS3ObjectMetadata(Path path)
            throws IOException
    {
        try {
            ObjectMetadata metadata =
                s3.getObjectMetadata(uri.getHost(), keyFromPath(path));
            if (metadata == null) {
                throw new FileNotFoundException("Path " + path.toString() + " not found");
            }
            return metadata;
        }
        catch (AmazonClientException e) {
            throw new FileNotFoundException(e.getMessage());
        }
    }

    private Path qualifiedPath(Path path)
    {
        return path.makeQualified(this.uri, getWorkingDirectory());
    }

    private Path makeAbsolute(Path path)
    {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDirectory, path);
    }

    private LocatedFileStatus createLocatedFileStatus(FileStatus status)
    {
        try {
            BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
            return new LocatedFileStatus(status, fakeLocation);
        }
        catch (IOException e) {
            throw propagate(e);
        }
    }

    private static String keyFromPath(Path path)
    {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        String key = nullToEmpty(path.toUri().getPath());
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        if (key.endsWith("/")) {
            key = key.substring(0, key.length() - 1);
        }
        return key;
    }

    private static AWSCredentials getAwsCredentials(URI uri, Configuration conf)
    {
        S3Credentials credentials = new S3Credentials();
        credentials.initialize(uri, conf);
        return new BasicAWSCredentials(credentials.getAccessKey(), credentials.getSecretAccessKey());
    }

    private static class PrestoS3InputStream
            extends FSInputStream
    {
        private final AmazonS3 s3;
        private final String host;
        private final Path path;

        private boolean closed;
        private S3ObjectInputStream in;
        private long position;
        private int maxClientRetry;

        public PrestoS3InputStream(AmazonS3 s3, String host, Path path, int maxClientRetry)
        {
            this.s3 = checkNotNull(s3, "s3 is null");
            this.host = checkNotNull(host, "host is null");
            this.path = checkNotNull(path, "path is null");
            this.maxClientRetry = checkNotNull(maxClientRetry, "max client retry is null");
        }

        @Override
        public void close()
                throws IOException
        {
            closed = true;
            if (in != null) {
                in.abort();
                in = null;
            }
        }

        @Override
        public void seek(long pos)
                throws IOException
        {
            checkState(!closed, "already closed");
            checkArgument(pos >= 0, "position is negative: %s", pos);

            if ((in != null) && (pos == position)) {
                // already at specified position
                return;
            }

            if ((in != null) && (pos > position)) {
                // seeking forwards
                long skip = pos - position;
                if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
                    // already buffered or seek is small enough
                    if (in.skip(skip) == skip) {
                        position = pos;
                        return;
                    }
                }
            }

            // close the stream and open at desired position
            if (in != null) {
                in.abort();
                in = null;
            }
            in = getS3Object(path, pos).getObjectContent();
            position = pos;
        }

        @Override
        public long getPos()
                throws IOException
        {
            return position;
        }

        @Override
        public int read()
                throws IOException
        {
            int b = -1;
            int numRetry = 0;
            while (b == -1 && numRetry < this.maxClientRetry) {
                try {
                    numRetry = numRetry + 1;
                    if (in == null) {
                        in = getS3Object(path, position).getObjectContent();
                    }
                    b = in.read();
                }
                catch (AmazonClientException | IOException e) {
                    log.error("Attempt " + numRetry + " gets Exception when reading from S3: " + e.getMessage());
                    if (numRetry >= this.maxClientRetry) {
                        throw new IOException(e);
                    }
                    close();
                    in = getS3Object(path, position).getObjectContent();
                }
            }
            if (b != -1) {
                position++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            int n = -1;
            int numRetry = 0;
            while (n == -1 && numRetry < this.maxClientRetry) {
                try {
                    numRetry = numRetry + 1;
                    if (in == null) {
                        in = getS3Object(path, position).getObjectContent();
                    }
                    n = in.read(b, off, len);
                }
                catch (AmazonClientException | IOException e) {
                    log.error("Attempt " + numRetry + " gets Exception when reading from S3: " + e.getMessage());
                    if (numRetry >= this.maxClientRetry) {
                        throw new IOException(e);
                    }
                    close();
                    in = getS3Object(path, position).getObjectContent();
                }
            }
            if (n != -1) {
                position += n;
            }
            return n;
        }

        @Override
        public boolean seekToNewSource(long targetPos)
                throws IOException
        {
            return false;
        }

        private S3Object getS3Object(Path path, long start)
                throws IOException
        {
            try {
                return s3.getObject(new GetObjectRequest(host, keyFromPath(path))
                        .withRange(start, Long.MAX_VALUE));
            }
            catch (AmazonClientException e) {
                throw new IOException(e);
            }
        }
    }
    private static class PrestoS3OutputStream
        extends OutputStream
    {
        private final AmazonS3 s3;
        private final String host;

        private Configuration conf;
        private String key;
        private File backupFile;
        private OutputStream backupStream;
        private boolean closed;

        public PrestoS3OutputStream(
            AmazonS3 s3, String host, Configuration conf,
            String key, Progressable progress, int bufferSize) throws IOException
        {
            this.s3 = checkNotNull(s3, "s3 is null");
            this.host = checkNotNull(host, "host is null");
            this.conf = checkNotNull(conf, "conf is null");
            this.key = checkNotNull(key, "key is null");
            this.backupFile = newBackupFile();
            log.debug("OutputStream for key '" + key + "' writing to tempfile '" + this.backupFile + "'");
            this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
        }

        private File newBackupFile() throws IOException
        {
            File dir = new File(conf.get("fs.s3.buffer.dir"));
            if (!dir.mkdirs() && !dir.exists()) {
                throw new IOException("Cannot create S3 buffer directory: " + dir);
            }
            File result = File.createTempFile("output-", ".tmp", dir);
            result.deleteOnExit();
            return result;
        }

        @Override
        public void flush() throws IOException
        {
            backupStream.flush();
        }

        @Override
        public synchronized void close() throws IOException
        {
            if (closed) {
                return;
            }
            backupStream.close();
            log.debug("OutputStream for key '" + key + "' closed. Now beginning upload");

            try {
                try {
                    PutObjectRequest request = new PutObjectRequest(host, key, backupFile);
                    this.s3.putObject(request);
                }
                catch (Exception e) {
                    // cleanup amazon S3 if putObject fail
                    this.s3.deleteObject(host, key);
                    throw new RuntimeException("Get Exception when put to S3", e);
                }
            }
            finally {
                if (!backupFile.delete()) {
                    log.warn("Could not delete temporary s3n file: " + backupFile);
                }
                super.close();
                closed = true;
            }
            log.debug("OutputStream for key '" + key + "' upload complete");
        }

        @Override
        public void write(int b) throws IOException
        {
            backupStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            backupStream.write(b, off, len);
        }
    }
}
