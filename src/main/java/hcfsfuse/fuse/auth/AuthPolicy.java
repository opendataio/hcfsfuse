package hcfsfuse.fuse.auth;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Auth Policy Interface.
 */
public interface AuthPolicy {
  /**
   * set use and group.
   * @param uri - path url
   */
  void setUserGroupIfNeeded(Path uri) throws IOException;
}
