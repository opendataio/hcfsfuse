# hcfsfuse

`hcfsfuse` is a fuse program which can access any hcfs implemented file system.
Such as local filesystem, hdfs, alluxio, oss, cos, s3, and so on.

# how to build

```bash
$ mvn clean package
```

# how to run

```bash
$ -config=core-site.xml -m /Users/mbl/fusefs -r file:///tmp/
```