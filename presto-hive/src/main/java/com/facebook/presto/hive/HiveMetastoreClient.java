package com.facebook.presto.hive;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;

class HiveMetastoreClient
        extends ThriftHiveMetastore.Client
        implements Closeable
{
    private final TTransport transport;

    HiveMetastoreClient(TTransport transport)
    {
        super(new TBinaryProtocol(transport));
        this.transport = transport;
    }

    @Override
    public void close()
    {
        transport.close();
    }

    public static HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        TSocket tSocket = new TSocket(host, port);
        tSocket.open();
        return new HiveMetastoreClient(tSocket);
    }
}
