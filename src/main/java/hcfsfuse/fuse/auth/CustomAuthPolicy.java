package hcfsfuse.fuse.auth;

import static hcfsfuse.fuse.AuthConstants.AUTH_POLICY_CUSTOM_GROUP;
import static hcfsfuse.fuse.AuthConstants.AUTH_POLICY_CUSTOM_USER;

import alluxio.jnifuse.FuseFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Custom Auth Policy.
 */
public class CustomAuthPolicy implements AuthPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(CustomAuthPolicy.class);
  private final FileSystem mFileSystem;
  private final String fUname;
  private final String fGname;

  /**
   * @param fileSystem - FileSystem
   * @param conf - Configuration
   * @param fuseFileSystem - FuseFileSystem
   */
  public CustomAuthPolicy(FileSystem fileSystem, Configuration conf,
      FuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    fUname = conf.get(AUTH_POLICY_CUSTOM_USER);
    fGname = conf.get(AUTH_POLICY_CUSTOM_GROUP);
  }

  @Override
  public void setUserGroupIfNeeded(Path uri) throws IOException {
    if (fUname == null || fGname == null) {
      return;
    }
    LOG.debug("Set attributes of path {} to {}, {}", uri, fUname, fGname);
    mFileSystem.setOwner(uri, fUname, fGname);
  }
}
