package hcfsfuse.fuse.auth;

import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.FuseFileSystem;
import alluxio.jnifuse.struct.FuseContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default Auth Policy.
 */
public class DefaultAuthPolicy implements AuthPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthPolicy.class);
  private static final String USER_NAME = System.getProperty("user.name");
  private static final String GROUP_NAME = System.getProperty("user.name");
  private static final long DEFAULT_UID = AlluxioFuseUtils.getUid(USER_NAME);
  private static final long DEFAULT_GID = AlluxioFuseUtils.getGid(GROUP_NAME);

  private final FileSystem mFileSystem;
  private final FuseFileSystem mFuseFileSystem;

  /**
   * @param fileSystem - FileSystem
   * @param conf - Configuration
   * @param fuseFileSystem - FuseFileSystem
   */
  public DefaultAuthPolicy(
      FileSystem fileSystem, Configuration conf, FuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mFuseFileSystem = fuseFileSystem;
  }

  @Override
  public void setUserGroupIfNeeded(Path uri) throws IOException {
    FuseContext fc = mFuseFileSystem.getContext();
    long uid = fc.uid.get();
    long gid = fc.gid.get();

    String gname = "";
    String uname = "";
    if (gid != DEFAULT_GID) {
      String groupName = AlluxioFuseUtils.getGroupName(gid);
      if (groupName.isEmpty()) {
        // This should never be reached since input gid is always valid
        LOG.error("Failed to get group name from gid {}, fallback to {}.", gid, GROUP_NAME);
        groupName = GROUP_NAME;
      }
      gname = groupName;
    }
    if (uid != DEFAULT_UID) {
      String userName = AlluxioFuseUtils.getUserName(uid);
      if (userName.isEmpty()) {
        // This should never be reached since input uid is always valid
        LOG.error("Failed to get user name from uid {}, fallback to {}", uid, USER_NAME);
        userName = USER_NAME;
      }
      uname = userName;
    }
    if (gid != DEFAULT_GID || uid != DEFAULT_UID) {
      LOG.debug("Set attributes of path {} to {}, {}", uri, gid, uid);
      mFileSystem.setOwner(uri, uname, gname);
    }
  }
}
