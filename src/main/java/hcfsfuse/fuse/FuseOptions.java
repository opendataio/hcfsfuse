package hcfsfuse.fuse;

import java.util.List;

/**
 * Convenience class to pass around HCFS-FUSE options.
 */
public class FuseOptions {

  private final String mountPoint;
  private final String root;
  private final boolean debug;
  private final boolean jniFuseEnable;
  private final List<String> fuseOpts;
  private final String[] confPaths;

  /**
   * Fuse options constructor.
   * @param mountPoint mountPoint
   * @param root root
   * @param debug debug
   * @param fuseOpts fuseOpts
   * @param confPaths confPaths
   * @param jniFuseEnable jniFuseEnable
   */
  public FuseOptions(String mountPoint, String root, boolean debug,
      List<String> fuseOpts, String[] confPaths, boolean jniFuseEnable) {
    this.mountPoint = mountPoint;
    this.root = root;
    this.debug = debug;
    this.fuseOpts = fuseOpts;
    this.confPaths = confPaths;
    this.jniFuseEnable = jniFuseEnable;
  }

  /**
   *
   * @return a list of fuse opts
   */
  public List<String> getFuseOpts() {
    return fuseOpts;
  }

  /**
   *
   * @return mountPoint
   */
  public String getMountPoint() {
    return mountPoint;
  }

  /**
   *
   * @return root
   */
  public String getRoot() {
    return root;
  }

  /**
   *
   * @return confPaths
   */
  public String[] getConfPaths() {
    return confPaths;
  }

  /**
   *
   * @return debug
   */
  public boolean isDebug() {
    return debug;
  }

  /**
   * @return true if jniFuse is enabled
   */
  public boolean isJniFuseEnable() {
    return jniFuseEnable;
  }
}
