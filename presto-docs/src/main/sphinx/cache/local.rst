=====================
Alluxio Cache Library
=====================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

Presto supports caching input data locally with a built-in cache
to reduce query latency using Hive Connector.
This cache, based on Alluixo client-side cache library,
utilizes the local storage (such as SSD) with configurable capacity and locations.
To understand its internals and the benchmark results of latency improvement,
please read this `article <https://prestodb.io/blog/2020/06/16/alluxio-datacaching>`_.
Note that this is a **read-cache**, **completely transparent** from users
and **fully managed** by individual Presto workers.
To provide Presto an independent, distributed cache service for read/write workloads with
customized data caching policies, please refer to :doc:`/cache/service`.


Setup
-----

To turn on this Alluxio local cache is very easy.
Simply include the following configuration in ``etc/catalog/hive.properties``
and restart Presto coordinator and workers:

.. code-block:: none

    hive.node-selection-strategy=SOFT_AFFINITY
    cache.enabled=true
    cache.type=ALLUXIO
    cache.alluxio.max-cache-size=500GB
    cache.base-directory=/tmp/alluxio-cache

In the above example configuration,

* ``hive.node-selection-strategy=SOFT_AFFINITY`` instructs Presto scheduler to take data affinity
  into consideration when secheduling tasks to workers that enables meaningful data caching effectiveness.
  This configuration property is defaul to ``NO_PREFERENCE`` and local data caching is only enabled when set to ``SOFT_AFFINITY``.
  Other configuration on coordinator that can impact data affinity includes
  ``node-scheduler.max-pending-splits-per-task`` (the max pending splits per task) and
  ``node-scheduler.max-splits-per-node`` (the max splits per node).
* ``cache.enabled=true`` turns on the local cache and ``cache.type=ALLUXIO`` sets it to Alluxio.
* ``cache.alluxio.max-cache-size=500GB`` sets storage space to be 500GB.
* ``cache.base-directory=/tmp/alluxio-cache`` specifies a local directory ``/tmp/alluxio-cache``. Note that this Presto server must have both read and write permission to access this local directory.


Monitoring
----------

This cache library is completely transparent to users.
To verify if the cache is working, one can check the directory set by ``cache.base-directory`` and see if temporary files are created there.
In addition, Alluxio exports various JMX metrics while performing various caching related operations.
System admins can also monitor the cache usage across the cluster by checking the following metrics:

* ``Client.CacheBytesEvicted``: Total number of bytes evicted from the client cache.
* ``Client.CacheBytesReadCache``:  Total number of bytes read from the client cache (e.g., cache hit).
* ``Client.CacheBytesRequestedExternal``: Total number of bytes the user requested to read which resulted in a cache miss. This number may be smaller than Client.CacheBytesReadExternal due to chunk reads.
* ``Client.CacheHitRate``: The hit rate measured by (# bytes read from cache) / (# bytes requested).
* ``Client.CacheSpaceAvailable``: Amount of bytes available in the client cache.
* ``Client.CacheSpaceUsed``: Amount of bytes used by the client cache.

Please refer to `Alluxio client metrics <https://docs.alluxio.io/os/user/stable/en/reference/Metrics-List.html#client-metrics>`_
for a full list of available metrics.
