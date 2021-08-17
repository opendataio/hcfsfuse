package hcfsfuse.fuse;

/**
 * Convenience class of auth options.
 */
public class Constants {
  public static final String AUTH_POLICY = "hcfs.fuse.auth.policy";
  public static final String AUTH_POLICY_CUSTOM = "custom";
  public static final String AUTH_POLICY_CUSTOM_USER = "hcfs.fuse.auth.custom.user";
  public static final String AUTH_POLICY_CUSTOM_GROUP = "hcfs.fuse.auth.custom.group";
  public static final String AUTH_POLICY_IGNORE_MKDIR_GROUP = "hcfs.fuse.ignore.mkdir.group";
  // jnr fuse max number of files that can be opened simultaneously
  public static final String JNR_OPEN_FILE_CONCURRENT = "hcfs.fuse.jnr.open.file.concurrent";
}
