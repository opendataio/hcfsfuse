package hcfsfuse.fuse;

import hcfsfuse.fuse.auth.AuthPolicy;
import hcfsfuse.fuse.auth.AuthPolicyFactory;

import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.FuseFillDir;
import alluxio.resource.LockResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Striped;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Main FUSE implementation class.
 * <p>
 * Implements the FUSE callbacks defined by jni-fuse.
 */
@ThreadSafe
public final class HCFSJniFuseFileSystem extends AbstractFuseFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(HCFSJniFuseFileSystem.class);
  private final FileSystem mFileSystem;
  private final Configuration mConf;
  private final Path mRootPath;
  private final LoadingCache<String, Path> mPathResolverCache;
  private final LoadingCache<String, Long> mUidCache;
  private final LoadingCache<String, Long> mGidCache;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final String mFsName;

  private static final int LOCK_SIZE = 2048;
  /** A readwrite lock pool to guard individual files based on striping. */
  private final Striped<ReadWriteLock> mFileLocks = Striped.readWriteLock(LOCK_SIZE);

  private final Map<Long, FSDataInputStream> mOpenFileEntries = new ConcurrentHashMap<>();
  private final Map<Long, FSDataOutputStream> mCreateFileEntries = new ConcurrentHashMap<>();
  private final boolean mIsUserGroupTranslation;
  private final AuthPolicy mAuthPolicy;

  // To make test build
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE = -1;
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;
  /**
   * df command will treat -1 as an unknown value.
   */
  @VisibleForTesting
  public static final int UNKNOWN_INODES = -1;
  /**
   * Most FileSystems on linux limit the length of file name beyond 255 characters.
   */
  @VisibleForTesting
  public static final int MAX_NAME_LENGTH = 255;

  private static final String USER_NAME = System.getProperty("user.name");
  private static final String GROUP_NAME = System.getProperty("user.name");
  private static final long DEFAULT_UID = AlluxioFuseUtils.getUid(USER_NAME);
  private static final long DEFAULT_GID = AlluxioFuseUtils.getGid(GROUP_NAME);

  /**
   * Creates a new instance of {@link HCFSJniFuseFileSystem}.
   *
   * @param fs target file system
   * @param fuseOptions options
   * @param conf configuration
   */
  public HCFSJniFuseFileSystem(
      FileSystem fs, FuseOptions fuseOptions, Configuration conf) {
    super(Paths.get(fuseOptions.getMountPoint()));
    mFsName = "hcfsJniFuse-" + ThreadLocalRandom.current().nextInt();
    mFileSystem = fs;
    mConf = conf;
    mRootPath = new Path(fuseOptions.getRoot());
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(500)
        .build(new CacheLoader<String, Path>() {
          @Override
          public Path load(String fusePath) {
            // fusePath is guaranteed to always be an absolute path (i.e., starts
            // with a fwd slash) - relative to the FUSE mount point
            String relPath = fusePath.substring(1);
            if (relPath.isEmpty()) {
              relPath = ".";
            }
            Path turi = new Path(mRootPath, relPath);
            return turi;
          }
        });
    mUidCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<String, Long>() {
          @Override
          public Long load(String userName) {
            return AlluxioFuseUtils.getUid(userName);
          }
        });
    mGidCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<String, Long>() {
          @Override
          public Long load(String groupName) {
            return AlluxioFuseUtils.getGidFromGroupName(groupName);
          }
        });
    mIsUserGroupTranslation = true;
    mAuthPolicy = AuthPolicyFactory.create(mFileSystem, conf, this);
  }

  @Override
  public int create(String path, long mode, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () ->
            createInternal(path, mode, fi),
        "create", "path=%s,mode=%o", path, mode);
  }

  private int createInternal(String path, long mode, FuseFileInfo fi) {
    final Path uri = mPathResolverCache.getUnchecked(path);
    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create {}: file name longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      FSDataOutputStream os =
          mFileSystem.create(mFileSystem, uri, new FsPermission((int) mode));
      long fid = mNextOpenFileId.getAndIncrement();
      mCreateFileEntries.put(fid, os);
      fi.fh.set(fid);
      mAuthPolicy.setUserGroupIfNeeded(uri);
    } catch (Throwable e) {
      LOG.error("Failed to create {}: ", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    return AlluxioFuseUtils.call(
        LOG, () -> getattrInternal(path, stat), "getattr", "path=%s", path);
  }

  private int getattrInternal(String path, FileStat stat) {
    final Path uri = mPathResolverCache.getUnchecked(path);
    try {
      FileStatus status = mFileSystem.getFileStatus(uri);
      long size = status.getLen();
      stat.st_size.set(size);

      // Sets block number to fulfill du command needs
      // `st_blksize` is ignored in `getattr` according to
      // https://github.com/libfuse/libfuse/blob/d4a7ba44b022e3b63fc215374d87ed9e930d9974/include/fuse.h#L302
      // According to http://man7.org/linux/man-pages/man2/stat.2.html,
      // `st_blocks` is the number of 512B blocks allocated
      stat.st_blocks.set((int) Math.ceil((double) size / 512));

      final long ctime_sec = status.getModificationTime() / 1000;
      // Keeps only the "residual" nanoseconds not caputred in citme_sec
      final long ctime_nsec = (status.getModificationTime() % 1000) * 1000;

      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);

      if (mIsUserGroupTranslation) {
        // Translate the file owner/group to unix uid/gid
        // Show as uid==-1 (nobody) if owner does not exist in unix
        // Show as gid==-1 (nogroup) if group does not exist in unix
        stat.st_uid.set(mUidCache.get(status.getOwner()));
        stat.st_gid.set(mGidCache.get(status.getGroup()));
      } else {
        stat.st_uid.set(DEFAULT_UID);
        stat.st_gid.set(DEFAULT_GID);
      }

      int mode = status.getPermission().toShort();
      if (status.isDirectory()) {
        mode |= FileStat.S_IFDIR;
      } else {
        mode |= FileStat.S_IFREG;
      }
      stat.st_mode.set(mode);
      stat.st_nlink.set(1);
    } catch (IOException e) {
      LOG.debug("Failed to get info of {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable e) {
      LOG.error("Failed to getattr {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int readdir(String path, long buff, long filter, long offset,
      FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readdirInternal(path, buff, filter, offset, fi),
        "readdir", "path=%s,buf=%s", path, buff);
  }

  private int readdirInternal(String path, long buff, long filter, long offset,
      FuseFileInfo fi) {
    final Path uri = mPathResolverCache.getUnchecked(path);
    try {
      // standard . and .. entries
      FuseFillDir.apply(filter, buff, ".", null, 0);
      FuseFillDir.apply(filter, buff, "..", null, 0);
      final FileStatus[] ls = mFileSystem.listStatus(uri);
      for (FileStatus file : ls) {
        FuseFillDir.apply(filter, buff, file.getPath().getName(), null, 0);
      }
    } catch (Throwable e) {
      LOG.error("Failed to readdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> openInternal(path, fi), "open", "path=%s", path);
  }

  private int openInternal(String path, FuseFileInfo fi) {
    final Path uri = mPathResolverCache.getUnchecked(path);
    final int flags = fi.flags.get();
    LOG.trace("open({}, 0x{}) [target: {}]", path, Integer.toHexString(flags), uri);
    try {
      long fd = mNextOpenFileId.getAndIncrement();
      if ((flags & 0b11) != 0) {
        FSDataOutputStream os =
            mFileSystem.create(uri);
        long fid = mNextOpenFileId.getAndIncrement();
        mCreateFileEntries.put(fid, os);
        fi.fh.set(fid);
        mAuthPolicy.setUserGroupIfNeeded(uri);
      } else {
        FSDataInputStream is = mFileSystem.open(uri);
        mOpenFileEntries.put(fd, is);
        fi.fh.set(fd);
      }
      return 0;
    } catch (Throwable e) {
      LOG.error("Failed to open {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readInternal(path, buf, size, offset, fi),
        "read", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int readInternal(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    int nread = 0;
    int rd = 0;
    final int sz = (int) size;
    long fd = fi.fh.get();
    // FileInStream is not thread safe
    try (LockResource r1 = new LockResource(mFileLocks.get(fd).writeLock())) {
      FSDataInputStream is = mOpenFileEntries.get(fd);
      if (is == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      if (offset - is.getPos() < is.available()) {
        is.seek(offset);
        final byte[] dest = new byte[sz];
        while (rd >= 0 && nread < size) {
          rd = is.read(dest, nread, sz - nread);
          if (rd >= 0) {
            nread += rd;
          }
        }

        if (nread == -1) { // EOF
          nread = 0;
        } else if (nread > 0) {
          buf.put(dest, 0, nread);
        }
      }
    } catch (Throwable e) {
      LOG.error("Failed to read, path: {} size: {} offset: {}", path, size, offset, e);
      return -ErrorCodes.EIO();
    }
    return nread;
  }

  @Override
  public int write(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> writeInternal(path, buf, size, offset, fi),
        "write", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int writeInternal(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    final int sz = (int) size;
    final long fd = fi.fh.get();
    FSDataOutputStream os = mCreateFileEntries.get(fd);
    if (os == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (offset < os.getPos()) {
      // no op
      return sz;
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(dest, 0, sz);
      os.write(dest);
    } catch (IOException e) {
      LOG.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    }
    return sz;
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> flushInternal(path, fi), "flush", "path=%s", path);
  }

  private int flushInternal(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> releaseInternal(path, fi), "release", "path=%s", path);
  }

  private int releaseInternal(String path, FuseFileInfo fi) {
    long fd = fi.fh.get();
    try (LockResource r1 = new LockResource(mFileLocks.get(fd).writeLock())) {
      FSDataInputStream is = mOpenFileEntries.remove(fd);
      FSDataOutputStream os = mCreateFileEntries.remove(fd);
      if (is == null && os == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      if (is != null) {
        is.close();
      }
      if (os != null) {
        os.close();
      }
    } catch (Throwable e) {
      LOG.error("Failed closing {}", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int mkdir(String path, long mode) {
    return AlluxioFuseUtils.call(LOG, () -> mkdirInternal(path, mode),
        "mkdir", "path=%s,mode=%o,", path, mode);
  }

  private int mkdirInternal(String path, long mode) {
    final Path uri = mPathResolverCache.getUnchecked(path);
    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create directory {}: name longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      mFileSystem.mkdirs(uri, new FsPermission((int) mode));
      mAuthPolicy.setUserGroupIfNeeded(uri);
    } catch (Throwable e) {
      LOG.error("Failed to mkdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int unlink(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "unlink", "path=%s", path);
  }

  @Override
  public int rmdir(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "rmdir", "path=%s", path);
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    final Path uri = mPathResolverCache.getUnchecked(path);

    try {
      mFileSystem.delete(uri, true);
    } catch (Throwable e) {
      LOG.error("Failed to delete {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int rename(String oldPath, String newPath) {
    return AlluxioFuseUtils.call(LOG, () -> renameInternal(oldPath, newPath),
        "rename", "oldPath=%s,newPath=%s,", oldPath, newPath);
  }

  private int renameInternal(String oldPath, String newPath) {
    final Path oldUri = mPathResolverCache.getUnchecked(oldPath);
    final Path newUri = mPathResolverCache.getUnchecked(newPath);
    final String name = newUri.getName();
    if (name.length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to rename {} to {}, name {} is longer than {} characters",
          oldPath, newPath, name, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      mFileSystem.rename(oldUri, newUri);
    } catch (Throwable e) {
      LOG.error("Failed to rename {} to {}: ", oldPath, newPath, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int chmod(String path, long mode) {
    return AlluxioFuseUtils.call(LOG, () -> chmodInternal(path, mode),
        "chmod", "path=%s,mode=%o", path, mode);
  }

  private int chmodInternal(String path, long mode) {
    Path uri = mPathResolverCache.getUnchecked(path);

    try {
      mFileSystem.setPermission(uri, new FsPermission((int) mode));
    } catch (Throwable t) {
      LOG.error("Failed to change {} to mode {}", path, mode, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  @Override
  public int chown(String path, long uid, long gid) {
    return AlluxioFuseUtils.call(LOG, () -> chownInternal(path, uid, gid),
        "chown", "path=%s,uid=%o,gid=%o", path, uid, gid);
  }

  private int chownInternal(String path, long uid, long gid) {
    if (!mIsUserGroupTranslation) {
      LOG.info("Cannot change the owner/group of path {}", path);
      return -ErrorCodes.EOPNOTSUPP();
    }

    try {
      final Path uri = mPathResolverCache.getUnchecked(path);

      String userName = "";
      if (uid != ID_NOT_SET_VALUE && uid != ID_NOT_SET_VALUE_UNSIGNED) {
        userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EINVAL();
        }
      }

      String groupName = "";
      if (gid != ID_NOT_SET_VALUE && gid != ID_NOT_SET_VALUE_UNSIGNED) {
        groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get group name from gid {}", gid);
          return -ErrorCodes.EINVAL();
        }
      } else if (!userName.isEmpty()) {
        groupName = AlluxioFuseUtils.getGroupName(userName);
      }

      if (userName.isEmpty() && groupName.isEmpty()) {
        // This should never be reached
        LOG.info("Unable to change owner and group of file {} when uid is {} and gid is {}", path,
            userName, groupName);
      } else if (userName.isEmpty()) {
        LOG.info("Change group of file {} to {}", path, groupName);
        mFileSystem.setOwner(uri, null, groupName);
      } else if (groupName.isEmpty()) {
        LOG.info("Change user of file {} to {}", path, userName);
        mFileSystem.setOwner(uri, userName, null);
      } else {
        LOG.info("Change owner of file {} to {}:{}", path, userName, groupName);
        mFileSystem.setOwner(uri, userName, groupName);
      }
    } catch (Throwable t) {
      LOG.error("Failed to chown {} to uid {} and gid {}", path, uid, gid, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  @Override
  public int truncate(String path, long size) {
    return 0;
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFileSystemName() {
    return mFsName;
  }

  @VisibleForTesting
  LoadingCache<String, Path> getPathResolverCache() {
    return mPathResolverCache;
  }
}
