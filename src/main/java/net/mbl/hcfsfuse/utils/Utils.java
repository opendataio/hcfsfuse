package net.mbl.hcfsfuse.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * @author leoncao
 * 2021/1/25
 */
public class Utils {
  /**
   * 获取当前进程ID.
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
   * 获取当前线程ID.
   * * @return Long
   */
  public static Long getThreadId() {
    try {
      return Thread.currentThread().getId();
    } catch (Exception e) {
      return null;
    }
  }
}
