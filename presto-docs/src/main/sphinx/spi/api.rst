==================
The Presto SPI API
==================

Implementing a new Presto connector is really just implementing and
overriding a set of methods on intefaces defined by the SPI. Doing so
gives Presto a structure for invoking your code to access any data
source. Even if that data source doesn't have underlying tables
backing your data, as long as you adapt your data source to the API
expected by Presto, you can write queries against this data.

In this chapter, we're going to walk through the several interfaces
that comprise the Presto SPI and discuss strategies for adapting this
API to your data source.

GitHub Code
-----------

The example Presto SPI can be found here: 

    https://github.com/facebook/presto/tree/master/presto-spi

Plugin Metadata
---------------

Each plugin identifies an entry point - an implementation of the
Plugin interface. This class is listed in a common location in every
SPI JAR archive. The plugin descriptor available on the classpath in
META-INF/services in a file named com.facebook.presto.spi.Plugin. The
contents of this file is a single line listing the name of the Plugin
class.

.. code-block:: none

    com.facebook.presto.example.ExamplePlugin

Plugin
---------------------

The plugin interface is a good place to start to understand the Presto
SPI. This interface provides a mechanism for setting optional
configuration. This is configuration that is going to be passed in via
a catalog configuration file and it is your connector's ability to
keep track of additional configuration that is specific to your
connector.  For example, if your connector is connecting to a
relational database, this is where you would keep track of
configuration parameters that point to specific ports or host names.

The getServices() method is a top-level function that Presto calls to
retrieve a ConnectorFactory when Presto is ready to create an instance
of this connector to support a catalog.

.. code-block:: java

    package com.facebook.presto.spi;

    import java.util.List;
    import java.util.Map;

    public interface Plugin
    {
	void setOptionalConfig(Map<String, String> optionalConfig);

    	<T> List<T> getServices(Class<T> type);
    }


ConnectorFactory
-------------------------------

When you implement the interfaces in the presto-spi project you are
creating a new Connector.  Instances of your Connector are created by
a ConnectorFactory instance which is created when Presto calls
getServices() on the plugin to request a service of type
ConnectorFactory.

The ConnectorFactory is a simple interface responsible for creating an instance of a Connector object and creating it with references to a series of lower-level managers.

.. code-block:: java

    package com.facebook.presto.spi;

    import java.util.Map;

    public interface ConnectorFactory
    {
        String getName();

        Connector create(String connectorId, Map<String, String> config);
    }

When a Connector is returned from the ConnectorFactory, Presto expects
the following services to be available form the Connector:

* ConnectorMetadata
* ConnectorSplitManager
* ConnectorRecordSetProvider
* ConnectorHandleResolver

The following sections give a brief overview of the purpose of each
interface:

Connector
^^^^^^^^^^^^^^^^^^^^^^^^^^^

While the Presto SPI revolves around the creation of Connector
instances, the interface itself isn't not very substantial. The
Connector interface supplies a single method ``getService()`` which
takes a class as an argument.

.. code-block:: java

    <T> T getService(Class<T> type);

This method is used to return managers and other supporting objects
that are involved in managing clients, metadata, splits, and other
objects related to the inner-workings of a connector.


ConnectorMetdata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ConnectorMetadata interface has a large number of important
methods that are responsible for allowing Presto to look at lists of
schemas, lists of tables, lists of columns, and other metadata about a
particular data source.

This interface is too big to list in this documentation, but if you
are interested in seeing strategies for implementing these methods
look at the example HTTP connector and the Cassandra connector. If
your underlying data source supports schemas, tables, and columns this
inteface should be straightforward to implement. If you are attempting
to adapt something that isn't a database (as the example HTTP
connector does) you may need to get creative about how you map
concepts to Presto's schema, table, and column concepts.

ConnectorSplitManger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The split manager is important for data sources that store data in
partitions. Take Hive as an example, when you query a large table with
Hive, Hive returns partitions that Presto then turns into splits which
are then distributed to tasks.

Here are the key functions to implement in the ConnectorSplitManager interface:

.. code-block:: java

    String getConnectorId();
    boolean canHandle(TableHandle handle);
    PartitionResult getPartitions(TableHandle table, TupleDomain tupleDomain);
    SplitSource getPartitionSplits(TableHandle table, List<Partition> partitions);

For data sources that don't have partitioned data, a good strategy
here is to simply return a single split for every table
requested. This is the strategy employed by the example HTTP
connector.


ConnectorRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Given a split and a list of columns, the record set provider is responsible for delivering data to Presto components.  Here's the interface of the ConnectorRecordSetProvider:

.. code-block: java

    boolean canHandle(Split split);
    RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns);
