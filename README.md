# hcfsfuse

`hcfsfuse` is a fuse program which can access any hcfs implemented file system.
Such as local filesystem, hdfs, ozone, alluxio, oss, cos, s3, and so on.

# how to build

A fat jar will be generated in the target folder named hcfsfuse-<VERSION>-jar-with-dependencies.jar after build the project successfully.

A Simple way like following

```bash
$ mvn clean package
```

If you want to specify the version of alluxio, hadoop or ozone, reference the following example.

```bash
 mvn clean package -Dhadoop.version=3.2.1 -Dozone.version=1.0.0
```

# how to run

```bash
$ java -Dlog4j.configuration=file:<LOG4j_FILE_PATH> -jar target/hcfsfuse-1.0.0-SNAPSHOT-jar-with-dependencies.jar -c core-site.xml -c another-site.xml -m /Users/mbl/fusefs -r file:///tmp/
```

Then, you can ls, touch, cp, rm, cat file or directory under `/Users/mbl/fusefs`.

If there are something wrong strangely, check if forgot unmount the mount point folder and unmount it manually as following.

```bash
umount /Users/mbl/fusefs
```

# Notice

**There are a lot of reference from Alluxio fuse.**