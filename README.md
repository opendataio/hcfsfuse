# hcfsfuse

`hcfsfuse` is a fuse program which can access any hcfs implemented file system.
Such as local filesystem, hdfs, alluxio, oss, cos, s3, and so on.

# how to build

```bash
$ mvn clean package
```

# how to run

```bash
$ java -jar target/hcfsfuse-1.0.0-SNAPSHOT-jar-with-dependencies.jar -config=core-site.xml -m /Users/mbl/fusefs -r file:///tmp/
```

then, you can ls, touch, cp, rm, cat file or directory under `/Users/mbl/fusefs`.

# Notice

**There are a lot of reference from Alluxio fuse.**