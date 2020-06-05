package net.mbl.hcfsfuse;

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseContext;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse.
 */
@Slf4j
public class HCFSFuseFileSystem extends FuseStubFS {

  private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
  private static final int MAX_OPEN_WAITTIME_MS = 5000;
  /**
   * Most FileSystems on linux limit the length of file name beyond 255 characters.
   */
  @VisibleForTesting
  public static final int MAX_NAME_LENGTH = 255;
  private static final long UID = HCFSFuseUtil.getUid(System.getProperty("user.name"));
  private static final long GID = HCFSFuseUtil.getGid(System.getProperty("user.name"));
  private final Path mRootPath;
  private final FileSystem mFileSystem;
  // Table of open files with corresponding InputStreams and OutputStreams
  private final IndexedSet<OpenFileEntry> mOpenFiles;
  private AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final LoadingCache<String, Path> mPathResolverCache;

  // Open file managements
  private static final IndexDefinition<OpenFileEntry, Long> ID_INDEX =
      new IndexDefinition<OpenFileEntry, Long>(true) {
        @Override
        public Long getFieldValue(OpenFileEntry o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<OpenFileEntry, String> PATH_INDEX =
      new IndexDefinition<OpenFileEntry, String>(true) {
        @Override
        public String getFieldValue(OpenFileEntry o) {
          return o.getPath();
        }
      };

    /**
   * Creates a new instance of {@link HCFSFuseFileSystem}.
   *
   * @param fs target file system
   * @param fuseOptions options
   * @param conf Alluxio configuration
   */
  public HCFSFuseFileSystem(FileSystem fs, FuseOptions fuseOptions,
      Configuration conf) throws IOException {
    mFileSystem = fs;
    mRootPath = new Path(fuseOptions.getRoot());
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(500)
        .build(new PathCacheLoader());
    mOpenFiles = new IndexedSet<>(ID_INDEX, PATH_INDEX);
  }

  @Override
  public int getattr(String path, FileStat stat) {
    int res = 0;
    final Path turi = mPathResolverCache.getUnchecked(path);
    try {
      FileStatus status = mFileSystem.getFileStatus(turi);
      stat.st_size.set(status.getLen());
      int mode = status.getPermission().toShort();
      if (status.isDirectory()) {
        mode |= FileStat.S_IFDIR;
      } else {
        mode |= FileStat.S_IFREG;
      }
      stat.st_mode.set(mode);
      final long ctime_sec = status.getModificationTime() / 1000;
      // Keeps only the "residual" nanoseconds not caputred in citme_sec
      final long ctime_nsec = (status.getModificationTime() % 1000) * 1000;

      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);
      stat.st_uid.set(HCFSFuseUtil.getUid(status.getOwner()));
      stat.st_gid.set(HCFSFuseUtil.getGidFromGroupName(status.getGroup()));
    } catch (IOException e) {
      log.debug("Failed to get info of {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    }

    return res;
  }

  /**
   * Reads the contents of a directory.
   *
   * @param path The FS path of the directory
   * @param buff The FUSE buffer to fill
   * @param filter FUSE filter
   * @param offset Ignored in fuse
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int readdir(String path, Pointer buff, FuseFillDir filter,
      @off_t long offset, FuseFileInfo fi) {
    final Path turi = mPathResolverCache.getUnchecked(path);
    log.trace("readdir({}) [target: {}]", path, turi);

    try {
      final FileStatus[] ls = mFileSystem.listStatus(turi);
      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (FileStatus file : ls) {
        filter.apply(buff, file.getPath().getName(), null, 0);
      }
    } catch (FileNotFoundException | InvalidPathException e) {
      log.debug("Failed to read directory {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      log.error("Failed to read directory {}", path, t);
      return -1;
    }

    return 0;
  }

  /**
   * Creates a new dir.
   *
   * @param path the path on the FS of the new dir
   * @param mode Dir creation flags (IGNORED)
   * @return 0 on success, a negative value on error
   */
  @Override
  public int mkdir(String path, @mode_t long mode) {
    final Path turi = mPathResolverCache.getUnchecked(path);
    log.trace("mkdir({}) [target: {}]", path, turi);
    if (turi.getName().length() > MAX_NAME_LENGTH) {
      log.error("Failed to create directory {}, directory name is longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    FuseContext fc = getContext();
    long uid = fc.uid.get();
    long gid = fc.gid.get();
    try {
      String groupName = HCFSFuseUtil.getGroupName(gid);
      if (groupName.isEmpty()) {
        // This should never be reached since input gid is always valid
        log.error("Failed to get group name from gid {}.", gid);
        return -ErrorCodes.EFAULT();
      }
      String userName = HCFSFuseUtil.getUserName(uid);
      if (userName.isEmpty()) {
        // This should never be reached since input uid is always valid
        log.error("Failed to get user name from uid {}", uid);
        return -ErrorCodes.EFAULT();
      }
      mFileSystem.mkdirs(turi, new FsPermission((int) mode));
      mFileSystem.setOwner(turi, userName, groupName);
    } catch (FileAlreadyExistsException e) {
      log.debug("Failed to create directory {}, directory already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      log.debug("Failed to create directory {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      log.error("Failed to create directory {}", path, t);
      return HCFSFuseUtil.getErrorCode(t);
    }

    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    final Path turi = mPathResolverCache.getUnchecked(path);
    // (see {@code man 2 open} for the structure of the flags bitfield)
    // File creation flags are the last two bits of flags
    final int flags = fi.flags.get();
    log.trace("open({}, 0x{}) [target: {}]", path, Integer.toHexString(flags), turi);
    if (mOpenFiles.size() >= MAX_OPEN_FILES) {
      log.error("Cannot open {}: too many open files (MAX_OPEN_FILES: {})", path, MAX_OPEN_FILES);
      return ErrorCodes.EMFILE();
    }
    FSDataInputStream is;
    try {
      is = mFileSystem.open(turi);
    } catch (Throwable t) {
      log.error("Failed to open file {}", path, t);
      if (t instanceof IOException) {
        return -ErrorCodes.EIO();
      } else {
        return -ErrorCodes.EBADMSG();
      }
    }
    long fid = mNextOpenFileId.getAndIncrement();
    mOpenFiles.add(new OpenFileEntry(fid, path, is, null));
    fi.fh.set(fid);

    return 0;
  }

  /**
   * Reads data from an open file.
   *
   * @param path the FS path of the file to read
   * @param buf FUSE buffer to fill with data read
   * @param size how many bytes to read. The maximum value that is accepted on this method is {@link
   * Integer#MAX_VALUE} (note that current FUSE implementation will call this method with a size of
   * at most 128K).
   * @param offset offset of the read operation
   * @param fi FileInfo data structure kept by FUSE
   * @return the number of bytes read or 0 on EOF. A negative value on error
   */
  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {

    if (size > Integer.MAX_VALUE) {
      log.error("Cannot read more than Integer.MAX_VALUE");
      return -ErrorCodes.EINVAL();
    }
    log.trace("read({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      log.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    int rd = 0;
    int nread = 0;
    if (oe.getIn() == null) {
      log.error("{} was not open for reading", path);
      return -ErrorCodes.EBADFD();
    }
    try {
      oe.getIn().seek(offset);
      final byte[] dest = new byte[sz];
      while (rd >= 0 && nread < size) {
        rd = oe.getIn().read(dest, nread, sz - nread);
        if (rd >= 0) {
          nread += rd;
        }
      }

      // EOF
      if (nread == -1) {
        nread = 0;
      } else if (nread > 0) {
        buf.put(0, dest, 0, nread);
      }
    } catch (Throwable t) {
      log.error("Failed to read file {}", path, t);
      return HCFSFuseUtil.getErrorCode(t);
    }

    return nread;
  }

  /**
   * Creates and opens a new file.
   *
   * @param path The FS path of the file to open
   * @param mode mode flags
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success. A negative value on error
   */
  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    final Path uri = mPathResolverCache.getUnchecked(path);
    final int flags = fi.flags.get();
    log.trace("create({}, {}) [target: {}]", path, Integer.toHexString(flags), uri);

    if (uri.getName().length() > MAX_NAME_LENGTH) {
      log.error("Failed to create {}, file name is longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      if (mOpenFiles.size() >= MAX_OPEN_FILES) {
        log.error("Cannot create {}: too many open files (MAX_OPEN_FILES: {})", path,
            MAX_OPEN_FILES);
        return -ErrorCodes.EMFILE();
      }
      FuseContext fc = getContext();
      long uid = fc.uid.get();
      long gid = fc.gid.get();

      String gname = "";
      String uname = "";
      if (gid != GID) {
        String groupName = HCFSFuseUtil.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached since input gid is always valid
          log.error("Failed to get group name from gid {}.", gid);
          return -ErrorCodes.EFAULT();
        }
        gname = groupName;
      }
      if (uid != UID) {
        String userName = HCFSFuseUtil.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached since input uid is always valid
          log.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EFAULT();
        }
        uname = userName;
      }
      FSDataOutputStream os = mFileSystem.create(mFileSystem, uri, new FsPermission((int) mode));
      long fid = mNextOpenFileId.getAndIncrement();
      mOpenFiles.add(new OpenFileEntry(fid, path, null, os));
      fi.fh.set(fid);
      if (gid != GID || uid != UID) {
        log.debug("Set attributes of path {} to {}, {}", path, gid, uid);
        mFileSystem.setOwner(uri, uname, gname);
      }
      log.debug("{} created and opened", path);
    } catch (FileAlreadyExistsException e) {
      log.debug("Failed to create {}, file already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      log.debug("Failed to create {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      log.error("Failed to create {}", path, t);
      return HCFSFuseUtil.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Flushes cached data on target.
   * <p>
   * Called on explicit sync() operation or at close().
   *
   * @param path The path on the FS of the file to close
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int flush(String path, FuseFileInfo fi) {
    log.trace("flush({})", path);
    final long fd = fi.fh.get();
    OpenFileEntry oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      log.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (oe.getOut() != null) {
      try {
        oe.getOut().flush();
      } catch (IOException e) {
        log.error("Failed to flush {}", path, e);
        return -ErrorCodes.EIO();
      }
    } else {
      log.debug("Not flushing: {} was not open for writing", path);
    }
    return 0;
  }

  /**
   * Releases the resources associated to an open file. Release() is async.
   * <p>
   * Guaranteed to be called once for each open() or create().
   *
   * @param path the FS path of the file to release
   * @param fi FileInfo data structure kept by FUSE
   * @return 0. The return value is ignored by FUSE (any error should be reported on flush instead)
   */
  @Override
  public int release(String path, FuseFileInfo fi) {
    log.trace("release({})", path);
    OpenFileEntry oe;
    final long fd = fi.fh.get();
    oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    mOpenFiles.remove(oe);
    if (oe == null) {
      log.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    try {
      oe.close();
    } catch (IOException e) {
      log.error("Failed closing {} [in]", path, e);
    }
    return 0;
  }

  /**
   * Renames a path.
   *
   * @param oldPath the source path in the FS
   * @param newPath the destination path in the FS
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rename(String oldPath, String newPath) {
    final Path oldUri = mPathResolverCache.getUnchecked(oldPath);
    final Path newUri = mPathResolverCache.getUnchecked(newPath);
    final String name = newUri.getName();
    log.trace("rename({}, {}) [target: {}, {}]", oldPath, newPath, oldUri, newUri);

    if (name.length() > MAX_NAME_LENGTH) {
      log.error("Failed to rename {} to {}, name {} is longer than {} characters",
          oldPath, newPath, name, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      mFileSystem.rename(oldUri, newUri);
      OpenFileEntry oe = mOpenFiles.getFirstByField(PATH_INDEX, oldPath);
      if (oe != null) {
        oe.setPath(newPath);
      }
    } catch (FileNotFoundException e) {
      log.debug("Failed to rename {} to {}, file {} does not exist", oldPath, newPath, oldPath);
      return -ErrorCodes.ENOENT();
    } catch (FileAlreadyExistsException e) {
      log.debug("Failed to rename {} to {}, file {} already exists", oldPath, newPath, newPath);
      return -ErrorCodes.EEXIST();
    } catch (Throwable t) {
      log.error("Failed to rename {} to {}", oldPath, newPath, t);
      return HCFSFuseUtil.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Writes a buffer to an open target file. Random write is not supported, so the offset argument
   * is ignored. Also, due to an issue in OSXFUSE that may write the same content at a offset
   * multiple times, the write also checks that the subsequent write of the same offset is ignored.
   *
   * @param buf The buffer with source data
   * @param size How much data to write from the buffer. The maximum accepted size for writes is
   * {@link Integer#MAX_VALUE}. Note that current FUSE implementation will anyway call write with at
   * most 128K writes
   * @param offset The offset where to write in the file (IGNORED)
   * @param fi FileInfo data structure kept by FUSE
   * @return number of bytes written on success, a negative value on error
   */
  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      log.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    log.trace("write({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      log.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (oe.getOut() == null) {
      log.error("{} already exists in target and cannot be overwritten."
          + " Please delete this file first.", path);
      return -ErrorCodes.EEXIST();
    }

    if (offset < oe.getWriteOffset()) {
      // no op
      return sz;
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(0, dest, 0, sz);
      oe.getOut().write(dest);
      oe.setWriteOffset(offset + size);
    } catch (IOException e) {
      log.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    }

    return sz;
  }

  /**
   * Changes the size of a file. This operation would not succeed because of target's write-once
   * model.
   */
  @Override
  public int truncate(String path, long size) {
    log.error("Truncate is not supported {}", path);
    return -ErrorCodes.EOPNOTSUPP();
  }

  /**
   * Deletes a file from the FS.
   *
   * @param path the FS path of the file
   * @return 0 on success, a negative value on error
   */
  @Override
  public int unlink(String path) {
    log.trace("unlink({})", path);
    return rmInternal(path);
  }

  @Override
  public int utimens(String path, Timespec[] timespec) {
    return 0;
  }

  /**
   * Deletes an empty directory.
   *
   * @param path The FS path of the directory
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rmdir(String path) {
    log.trace("rmdir({})", path);
    return rmInternal(path);
  }

  /**
   * Changes the mode of a remote file.
   *
   * @param path the path of the file
   * @param mode the mode to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chmod(String path, @mode_t long mode) {
    final Path turi = mPathResolverCache.getUnchecked(path);
    try {
      mFileSystem.setPermission(turi, new FsPermission((int) mode));
    } catch (IOException e) {
      log.error("Failed to chmod {}", path, e);
      return HCFSFuseUtil.getErrorCode(e);
    }
    return 0;
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    final Path turi = mPathResolverCache.getUnchecked(path);

    try {
      mFileSystem.delete(turi, true);
    } catch (FileNotFoundException | InvalidPathException e) {
      log.debug("Failed to remove {}, file does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      log.error("Failed to remove {}", path, t);
      return HCFSFuseUtil.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Resolves a FUSE path into {@link Path} and possibly keeps it in the cache.
   */
  private final class PathCacheLoader extends CacheLoader<String, Path> {

    /**
     * Constructs a new {@link PathCacheLoader}.
     */
    PathCacheLoader() {
    }

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
  }
}
