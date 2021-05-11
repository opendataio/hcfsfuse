package hcfsfuse.fuse.auth;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Auth Policy.
 */
public class CustomAuthPolicy extends DefaultAuthPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthPolicy.class);

  /**
   * set use and group.
   * @param uri - path url
   * @param mFileSystem - FileSystem
   * @param uname - user name
   * @param gname - group name
   * @throws Exception
   */
  public void setUserGroupIfNeeded(Path uri, FileSystem mFileSystem, String uname, String gname)
      throws Exception {
    if (uname == null || gname == null) {
      LOG.error("if enable tbds auth policy, user name or group name can not be null");
      throw new Exception("if enable tbds auth policy, user name or group name can not be null");
    }
    LOG.debug("Set attributes of path {} to {}, {}", uri, uname, gname);
    mFileSystem.setOwner(uri, uname, gname);
  }
}
