package net.mbl.hcfsfuse;

import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;

import lombok.extern.slf4j.Slf4j;
import ru.serce.jnrfuse.ErrorCodes;

import java.io.IOException;

/**
 * Utility methods for Hcfs-FUSE.
 */
@Slf4j
public class HCFSFuseUtil {

  /**
   * Retrieves the uid of the given user.
   *
   * @param userName the user name
   * @return uid
   */
  public static long getUid(String userName) {
    return getIdInfo("-u", userName);
  }

  /**
   * Retrieves the primary gid of the given user.
   *
   * @param userName the user name
   * @return gid
   */
  public static long getGid(String userName) {
    return getIdInfo("-g", userName);
  }

  /**
   * Retrieves the gid of the given group.
   *
   * @param groupName the group name
   * @return gid
   */
  public static long getGidFromGroupName(String groupName) throws IOException {
    String result = "";
    if (OSUtils.isLinux()) {
      String script = "getent group " + groupName + " | cut -d: -f3";
      result = ShellUtils.execCommand("bash", "-c", script).trim();
    } else if (OSUtils.isMacOS()) {
      String script = "dscl . -read /Groups/" + groupName
          + " | awk '($1 == \"PrimaryGroupID:\") { print $2 }'";
      result = ShellUtils.execCommand("bash", "-c", script).trim();
    }
    try {
      return Long.parseLong(result);
    } catch (NumberFormatException e) {
      log.error("Failed to get gid from group name {}.", groupName);
      return -1;
    }
  }

  /**
   * Runs the "id" command with the given options on the passed username.
   *
   * @param option option to pass to id (either -u or -g)
   * @param username the username on which to run the command
   * @return the uid (-u) or gid (-g) of username
   */
  private static long getIdInfo(String option, String username) {
    String output;
    try {
      output = ShellUtils.execCommand("id", option, username).trim();
    } catch (IOException e) {
      log.error("Failed to get id from {} with option {}", username, option);
      return -1;
    }
    return Long.parseLong(output);
  }

  /**
   * Gets the user name from the user id.
   *
   * @param uid user id
   * @return user name
   */
  public static String getUserName(long uid) throws IOException {
    return ShellUtils.execCommand("id", "-nu", Long.toString(uid)).trim();
  }

  /**
   * Gets the primary group name from the user name.
   *
   * @param userName the user name
   * @return group name
   */
  public static String getGroupName(String userName) throws IOException {
    return ShellUtils.execCommand("id", "-ng", userName).trim();
  }

  /**
   * Gets the group name from the group id.
   *
   * @param gid the group id
   * @return group name
   */
  public static String getGroupName(long gid) throws IOException {
    if (OSUtils.isLinux()) {
      String script = "getent group " + gid + " | cut -d: -f1";
      return ShellUtils.execCommand("bash", "-c", script).trim();
    } else if (OSUtils.isMacOS()) {
      String script = "dscl . list /Groups PrimaryGroupID | awk '($2 == \""
          + gid + "\") { print $1 }'";
      return ShellUtils.execCommand("bash", "-c", script).trim();
    }
    return "";
  }

  /**
   * Checks whether fuse is installed in local file system. Hcfs-Fuse only support mac and
   * linux.
   *
   * @return true if fuse is installed, false otherwise
   */
  public static boolean isFuseInstalled() {
    try {
      if (OSUtils.isLinux()) {
        String result = ShellUtils.execCommand("fusermount", "-V");
        return !result.isEmpty();
      } else if (OSUtils.isMacOS()) {
        String result = ShellUtils.execCommand("bash", "-c",
            "pkgutil --pkgs | grep -i com.github.osxfuse.pkg.Core");
        return !result.isEmpty();
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  /**
   * Gets the corresponding error code of a throwable.
   *
   * @param t throwable
   * @return the corresponding error code
   */
  public static int getErrorCode(Throwable t) {
    // Error codes and their explanations are described in
    // the Errno.java in jnr-constants
    if (t instanceof IOException) {
      return -ErrorCodes.EIO();
    } else {
      return -ErrorCodes.EBADMSG();
    }
  }
}
