package hcfsfuse.fuse.auth;

import alluxio.jnifuse.struct.FuseContext;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Auth Policy Interface.
 */
public interface AuthPolicy {
  /**
   * set use and group.
   * @param uri - path url
   * @param mFileSystem - FileSystem
   * @param fc - FuseContext
   * @throws Exception
   */
  void setUserGroupIfNeeded(Path uri, FileSystem mFileSystem, FuseContext fc) throws Exception;
}
