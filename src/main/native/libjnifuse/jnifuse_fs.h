#ifndef FUSE_NATIVE_LIBJNIFUSE_FS_H_
#define FUSE_NATIVE_LIBJNIFUSE_FS_H_

#include <jni.h>

#include "operation.h"

namespace jnifuse {

class CreateOperation;
class FlushOperation;
class GetattrOperation;
class MkdirOperation;
class OpenOperation;
class Operation;
class ReaddirOperation;
class ReadOperation;
class ReleaseOperation;
class RmdirOperation;
class UnlinkOperation;
class WriteOperation;
class RenameOperation;

class JniFuseFileSystem {
 private:
  JniFuseFileSystem(JNIEnv *env, jobject obj);
  ~JniFuseFileSystem();

 public:
  static JniFuseFileSystem *getInstance();
  static void init(JNIEnv *env, jobject obj);
  JNIEnv *getEnv();
  JavaVM *getJVM();
  jobject getFSObj();

 private:
  static JniFuseFileSystem *instance;
  JavaVM *jvm;
  jobject fs;

 public:
  GetattrOperation *getattrOper;
  OpenOperation *openOper;
  ReadOperation *readOper;
  ReaddirOperation *readdirOper;
  UnlinkOperation *unlinkOper;
  FlushOperation *flushOper;
  ReleaseOperation *releaseOper;
  CreateOperation *createOper;
  MkdirOperation *mkdirOper;
  RmdirOperation *rmdirOper;
  WriteOperation *writeOper;
  RenameOperation *renameOper;
};

}  // namespace jnifuse

#endif  // FUSE_NATIVE_LIBJNIFUSE_FS_H_
