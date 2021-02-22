package hcfsfuse.fuse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * Utility methods for HCFS-FUSE.
 */
public class HCFSFuseUtil {
  public static final Logger LOG =
      LoggerFactory.getLogger(HCFSFuseUtil.class);

  /**
   * Get current process id.
   * @return Long
   */
  public static Long getProcessId() {
    try {
      RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
      String name = runtime.getName();
      String pid = name.substring(0, name.indexOf('@'));
      return Long.parseLong(pid);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Get current thread id.
   * @return Long
   */
  public static Long getThreadId() {
    try {
      return Thread.currentThread().getId();
    } catch (Exception e) {
      return null;
    }
  }
}
