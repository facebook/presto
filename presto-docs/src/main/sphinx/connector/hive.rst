==============
Hive Connector
==============

The Hive connector allows querying data stored in a Hive
data warehouse. Hive is a combination of three components:

* Data files in varying formats that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database such as MySQL and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Supported File Types
--------------------

The following file types are supported for the Hive connector:

* ORC
* Parquet
* RCFile
* SequenceFile
* Text

Configuration
-------------

Presto includes Hive connectors for multiple versions of Hadoop:

* ``hive-hadoop1``: Apache Hadoop 1.x
* ``hive-hadoop2``: Apache Hadoop 2.x
* ``hive-cdh4``: Cloudera CDH 4
* ``hive-cdh5``: Cloudera CDH 5

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-cdh4`` connector as the ``hive`` catalog,
replacing ``hive-cdh4`` with the proper connector for your version
of Hadoop and ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive Clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

HDFS Configuration
^^^^^^^^^^^^^^^^^^

For basic setups, Presto configures the HDFS client automatically and
does not require any configuration files. In some cases, such as when using
federated HDFS or NameNode high availability, it is necessary to specify
additional HDFS client options in order to access your HDFS cluster. To do so,
add the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if necessary for your setup.
We also recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

The configuration files must exist on all Presto nodes. If you are
referencing existing Hadoop config files, make sure to copy them to
any Presto nodes that are not running Hadoop.

HDFS Username
^^^^^^^^^^^^^

When not using Kerberos with HDFS, Presto will access HDFS using the
OS user of the Presto process. For example, if Presto is running as
``nobody``, it will access HDFS as ``nobody``. You can override this
username by setting the ``HADOOP_USER_NAME`` system property in the
Presto :ref:`presto_jvm_config`, replacing ``hdfs_user`` with the
appropriate username:

.. code-block:: none

    -DHADOOP_USER_NAME=hdfs_user

Accessing Hadoop clusters protected with Kerberos authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kerberos authentication is currently supported for both HDFS and the Hive
metastore.

However there are still a few limitations:

* Kerberos authentication is only supported for the ``hive-hadoop2`` and
  ``hive-cdh5`` connectors.
* Kerberos authentication by ticket cache is not yet supported.

The properties that apply to Hive connector security are listed in the
`Configuration Properties`_ table. Please see the
:doc:`/connector/hive-security` section for a more detailed discussion of the
security options in the Hive connector.

Configuration Properties
------------------------

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default and the rest of the URIs are
                                                   fallback metastores. This property is required.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.
                                                   Example: ``/etc/hdfs-site.xml``

``hive.storage-format``                            The default file format used when creating new tables.       ``RCBINARY``

``hive.compression-codec``                         The compression codec to use when writing files.             ``GZIP``

``hive.force-local-scheduling``                    Force splits to be scheduled on the same node as the Hadoop  ``false``
                                                   DataNode process serving the split data.  This is useful for
                                                   installations where Presto is collocated with every
                                                   DataNode.

``hive.allow-drop-table``                          Allow the Hive connector to drop tables.                     ``false``

``hive.allow-rename-table``                        Allow the Hive connector to rename tables.                   ``false``

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Presto format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100

``hive.metastore.authentication.type``             Hive metastore authentication type.                          ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.metastore.service.principal``               The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore service.

``hive.metastore.client.keytab``                   Hive metastore client keytab location.

``hive.hdfs.authentication.type``                  HDFS authentication type.                                    ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.hdfs.impersonation.enabled``                Enable HDFS end user impersonation.                          ``false``

``hive.hdfs.presto.principal``                     The Kerberos principal that Presto will use when connecting
                                                   to HDFS.

``hive.hdfs.presto.keytab``                        HDFS client keytab location.

``hive.security``                                  See :doc:`hive-security`.

``security.config-file``                           Path of config file to use when ``hive.security=file``.
                                                   See :ref:`hive-file-based-authorization` for details.
================================================== ============================================================ ==========

Querying Hive Tables
--------------------

The following table is an example Hive table from the `Hive Tutorial`_.
It can be created in Hive (not in Presto) using the following
Hive ``CREATE TABLE`` command:

.. _Hive Tutorial: https://cwiki.apache.org/confluence/display/Hive/Tutorial#Tutorial-UsageandExamples

.. code-block:: none

    hive> CREATE TABLE page_view (
        >   viewTime INT,
        >   userid BIGINT,
        >   page_url STRING,
        >   referrer_url STRING,
        >   ip STRING COMMENT 'IP Address of the User')
        > COMMENT 'This is the page view table'
        > PARTITIONED BY (dt STRING, country STRING)
        > STORED AS SEQUENCEFILE;
    OK
    Time taken: 3.644 seconds

Assuming that this table was created in the ``web`` schema in
Hive, this table can be described in Presto::

    DESCRIBE hive.web.page_view;

.. code-block:: none

        Column    |  Type   | Null | Partition Key |        Comment
    --------------+---------+------+---------------+------------------------
     viewtime     | bigint  | true | false         |
     userid       | bigint  | true | false         |
     page_url     | varchar | true | false         |
     referrer_url | varchar | true | false         |
     ip           | varchar | true | false         | IP Address of the User
     dt           | varchar | true | true          |
     country      | varchar | true | true          |
    (7 rows)

This table can then be queried in Presto::

    SELECT * FROM hive.web.page_view;

Hive Connector Limitations
--------------------------

:doc:`/sql/delete` is only supported if the ``WHERE`` clause matches entire partitions.


Hive Data Stored in Amazon S3
-----------------------------

The Hive Connector can read data from files stored in S3, assuming the Hive
Metastore has tables defined as "external" which have locations defined as
S3 URIs.

Presto registers its own S3 filesystem for the following URI schemes:
``s3://``, ``s3a://``, ``s3n://`` and it registers the default Hadoop S3
FileSystem (``org.apache.hadoop.fs.s3.S3FileSystem``) for ``s3bfs://`` URIs.
The following assumes you are using the native Presto S3 FileSystem implementation.

S3 Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^

============================================ ====================================================== ==========
Property Name                                Description                                            Default
============================================ ====================================================== ==========
``hive.s3.use-instance-credentials``         Use the EC2 metadata service to retrieve API           ``true``
                                             credentials. This works with IAM roles in EC2.

``hive.s3.access-key``                       Default API access key to use

``hive.s3.secret-key``                       Default API secret key to use

``hive.s3.staging-directory``                Local staging directory for data written to S3         ``java.io.tmpdir``

``hive.s3.pin-client-to-current-region``     Pin S3 requests to the same region as the EC2 instance ``false``
                                             where Presto is running.

``hive.s3.sse.enabled``                      Enable S3 server-side encryption.                      ``false``

``hive.s3.endpoint``                         The S3 storage endpoint server. This can be used to
                                             connect to an S3-compatible storage system instead of
                                             AWS.

``hive.s3.signer-type``                      Specify a different signer type for S3-compatible
                                             storage. Example: ``S3SignerType`` for v2 signer type

``hive.s3.ssl.enabled``                      Use HTTPS to communicate with the S3 API               ``true``

``hive.s3.sse.enabled``                      Use S3 server-side encryption                          ``false``

``hive.s3.kms-key-id``                       If set, use S3 client-side encryption and use the AWS
                                             KMS to store encryption keys and use the value of
                                             this property as the KMS Key ID for newly created
                                             objects.

``hive.s3.encryption-materials-provider``    If set, use S3 client-side encryption and use the
                                             value of this property as the fully qualified name of
                                             a java class which implements the S3 Java SDK's
                                             ``EncryptionMaterialsProvider`` interface. If the
                                             class also implements ``Configurable`` from the Hadoop
                                             API, the Hadoop configuration will be passed in after
                                             the object has been created.
============================================ ====================================================== ==========

If you are running Presto on Amazon EC2 using EMR or another facility, it is highly recommended that you
set ``hive.s3.use-instance-credentials`` to ``true`` and use IAM Roles for EC2 to govern access to S3.
If this is the case, your EC2 instances will need to be assigned an IAM Role which grants appropriate
access to the data stored in the S3 bucket(s) you wish to use. This is much cleaner than setting AWS
access and secret keys in the ``hive.s3.access-key`` and ``hive.s3.secret-key`` settings, and also
allows EC2 to automatically rotate credentials on a regular basis without any additional work on your part.


S3 Tuning Properties
^^^^^^^^^^^^^^^^^^^^

The following tuning properties affect how many retries are attempted when communicating with S3, etc.
Most of these parameters affect settings on the ``ClientConfiguration`` object associated with
the ``AmazonS3Client``.

============================================ ====================================================== ==========
Property Name                                Description                                            Default
============================================ ====================================================== ==========
``hive.s3.max-error-retries``                Max number of error retries, set on the S3 client      ``10``

``hive.s3.max-client-retries``               Max number of read attempts to retry                   ``3``

``hive.s3.max-backoff-time``                 Use exponential backoff starting at 1 second up to     ``10 minutes``
                                             this maximum value when communicating with S3

``hive.s3.max-retry-time``                   Maximum time to retry communicating with S3            ``10 minutes``

``hive.s3.connect-timeout``                  TCP connection timeout                                 ``5 seconds``

``hive.s3.socket-timeout``                   TCP socket read timeout                                ``5 seconds``

``hive.s3.max-connections``                  Max number of simultaneous open connections to S3      ``500``

``hive.s3.multipart.min-file-size``          Min file size before multi-part upload to S3 is used   ``16 MB``

``hive.s3.multipart.min-part-size``          Multi-part upload part size                            ``5 MB``
============================================ ====================================================== ==========

S3 Data Encryption
^^^^^^^^^^^^^^^^^^

Presto supports reading and writing encrypted data in S3 using both server-side encryption with
S3 managed keys and client-side encryption using either the Amazon KMS or a software plugin to
manage AES encryption keys.

With `S3 server-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html>`_,
(known as "SSE-S3" in the Amazon documentation) the S3 infrastructure takes care of all encryption and decryption
work (with the exception of SSL to the client, assuming you have ``hive.s3.ssl.enabled`` set to ``true``).
S3 also manages all the encryption keys for you. To enable this, set ``hive.s3.sse.enabled`` to ``true``.

With `S3 client-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html>`_,
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure. Data is encrypted
and decrypted by Presto instead of in the S3 infrastructure. In this case, encryption keys can either by
managed using the AWS KMS or your own key management system. To use the AWS KMS for key management, set
``hive.s3.kms-key-id`` to the UUID of a KMS Key. Your AWS credentials or EC2 IAM Role will need to be
granted permission to use the given key as well.

To use a custom encryption key management system, set ``hive.s3.encryption-materials-provider`` to the
fully qualified name of a class which implements the
`EncryptionMaterialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/EncryptionMaterialsProvider.html>`_
interface from the AWS Java SDK. This class will have to be accessible to the Hive Connector through the
classpath and must be able to communicate with your custom key management system. If this class also implements
the ``org.apache.hadoop.conf.Configurable`` interface from the Hadoop Java API, then the Hadoop configuration
will be passed in after the object instance is created and before it is asked to provision or retrieve any
encryption keys.

