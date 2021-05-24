package hcfsfuse.fuse.auth;

import static hcfsfuse.fuse.AuthConstants.AUTH_POLICY;
import static hcfsfuse.fuse.AuthConstants.AUTH_POLICY_CUSTOM;

import alluxio.jnifuse.AbstractFuseFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Auth Policy Factory.
 */
public class AuthPolicyFactory {

  /**
   * @param filesystem - FileSystem
   * @param conf - Configuration
   * @param fuseFileSystem - FuseFileSystem
   * @return AuthPolicy
   */
  public static AuthPolicy create(FileSystem filesystem,
      Configuration conf,
      AbstractFuseFileSystem fuseFileSystem) {
    // TODO(maobaolong) using reflection to create instances dynamically.
    String authPolicy = conf.get(AUTH_POLICY, "default");
    if (authPolicy.equalsIgnoreCase(AUTH_POLICY_CUSTOM)) {
      return new CustomAuthPolicy(filesystem, conf, fuseFileSystem);
    } else {
      return new DefaultAuthPolicy(filesystem, conf, fuseFileSystem);
    }
  }
}
