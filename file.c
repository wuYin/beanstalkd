#include "dat.h"
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

static int  readrec(File*, Job *, int*);
static int  readrec5(File*, Job *, int*);
static int  readfull(File*, void*, int, int*, char*);
static void warnpos(File*, int, char*, ...)
__attribute__((format(printf, 3, 4)));

FAlloc *falloc = &rawfalloc;

enum
{
    Walver5 = 5
};

typedef struct Jobrec5 Jobrec5;

struct Jobrec5 {
    uint64 id;
    uint32 pri;
    uint64 delay; // usec
    uint64 ttr; // usec
    int32  body_size;
    uint64 created_at; // usec
    uint64 deadline_at; // usec
    uint32 reserve_ct;
    uint32 timeout_ct;
    uint32 release_ct;
    uint32 bury_ct;
    uint32 kick_ct;
    byte   state;

    char pad[1];
};

enum
{
	Jobrec5size = offsetof(Jobrec5, pad)
};

// rawfalloc allocates disk space of len bytes.
// It expects fd's offset to be 0; may also reset fd's offset to 0.
// Returns 0 on success, and a positive errno otherwise.
// 以 4KB 为单位，向 fd 中预写入 len 字节的 NULL 数据
int
rawfalloc(int fd, int len)
{
    // We do not use ftruncate() because it might extend the file
    // with a sequence of null bytes or a hole.
    // posix_fallocate() is not portable enough, might fail for NFS.
    static char buf[4096] = {0};
    int i, w;

    for (i = 0; i < len; i += w) {
        w = write(fd, buf, sizeof buf);
        if (w == -1)
            return errno;
    }
    lseek(fd, 0, 0);            // do not care if this fails
    return 0;
}

void
fileincref(File *f)
{
    if (!f) return;
    f->refs++;
}


void
filedecref(File *f)
{
    if (!f) return;
    f->refs--;
    if (f->refs < 1) {
        walgc(f->w);
    }
}


void
fileaddjob(File *f, Job *j)
{
    Job *h;

    h = &f->jlist;
    if (!h->fprev) h->fprev = h;
    j->file = f; // 绑定 j 的 binlog file
    j->fprev = h->fprev;
    j->fnext = h;
    h->fprev->fnext = j;
    h->fprev = j;
    fileincref(f);
}


void
filermjob(File *f, Job *j)
{
    if (!f) return;
    if (f != j->file) return;
    j->fnext->fprev = j->fprev;
    j->fprev->fnext = j->fnext;
    j->fnext = 0;
    j->fprev = 0;
    j->file = NULL;
    f->w->alive -= j->walused;
    j->walused = 0;
    filedecref(f);
}


// Fileread reads jobs from f->path into list.
// It returns 0 on success, or 1 if any errors occurred.
int
fileread(File *f, Job *list)
{
    int err = 0, v;

    // 1. 读 int 前 4 个字节，即 binlog 版本号
    if (!readfull(f, &v, sizeof(v), &err, "version")) {
        return err;
    }
    switch (v) {
    case Walver:
        fileincref(f);
        while (readrec(f, list, &err));
        filedecref(f);
        return err;
    case Walver5:
        fileincref(f);
        // 2. 逐个读取此 binlog 中的所有 job rec
        while (readrec5(f, list, &err));
        filedecref(f);
        return err;
    }

    warnx("%s: unknown version: %d", f->path, v);
    return 1;
}


// Readrec reads a record from f->fd into linked list l.
// If an error occurs, it sets *err to 1.
// Readrec returns the number of records read, either 1 or 0.
// [tube_name_len] [tube_name] [job_record]
static int
readrec(File *f, Job *l, int *err)
{
    int r, sz = 0;
    int namelen;
    Jobrec jr;
    Job *j;
    Tube *t;
    char tubename[MAX_TUBE_NAME_LEN];

    // 1. tube_name 长度
    r = read(f->fd, &namelen, sizeof(int));
    if (r == -1) {
        twarn("read");
        warnpos(f, 0, "error");
        *err = 1;
        return 0;
    }
    if (r != sizeof(int)) {
        return 0;
    }
    sz += r;
    if (namelen >= MAX_TUBE_NAME_LEN) {
        warnpos(f, -r, "namelen %d exceeds maximum of %d", namelen, MAX_TUBE_NAME_LEN - 1);
        *err = 1;
        return 0;
    }

    if (namelen < 0) {
        warnpos(f, -r, "namelen %d is negative", namelen);
        *err = 1;
        return 0;
    }

    // 2. tube_name
    if (namelen) {
        r = readfull(f, tubename, namelen, err, "tube name");
        if (!r) {
            return 0;
        }
        sz += r;
    }
    tubename[namelen] = '\0'; // 截断

    // 3. Jobrec
    r = readfull(f, &jr, sizeof(Jobrec), err, "job struct");
    if (!r) {
        return 0;
    }
    sz += r;

    // are we reading trailing zeroes?
    if (!jr.id) return 0;

    j = job_find(jr.id);
    if (!(j || namelen)) {
        // We read a short record without having seen a
        // full record for this job, so the full record
        // was in an earlier file that has been deleted.
        // Therefore the job itself has either been
        // deleted or migrated; either way, this record
        // should be ignored.
        return 1;
    }

    switch (jr.state) {
    case Reserved:
        jr.state = Ready;
        /* Falls through */
    case Ready:
    case Buried:
    case Delayed:
        // job 首次出现则放入 tube
        if (!j) {
            if ((size_t)jr.body_size > job_data_size_limit) {
                warnpos(f, -r, "job %"PRIu64" is too big (%"PRId32" > %zu)",
                        jr.id,
                        jr.body_size,
                        job_data_size_limit);
                goto Error;
            }
            t = tube_find_or_make(tubename);
            j = make_job_with_id(jr.pri, jr.delay, jr.ttr, jr.body_size,
                                 t, jr.id);
            job_list_reset(j);
            j->r.created_at = jr.created_at;
        }
        j->r = jr;
        job_list_insert(l, j); // 新插入 l

        // full record; read the job body
        if (namelen) {
            // job body 不应该变化
            if (jr.body_size != j->r.body_size) {
                warnpos(f, -r, "job %"PRIu64" size changed", j->r.id);
                warnpos(f, -r, "was %d, now %d", j->r.body_size, jr.body_size);
                goto Error;
            }
            r = readfull(f, j->body, j->r.body_size, err, "job body");
            if (!r) {
                goto Error;
            }
            sz += r;

            // since this is a full record, we can move
            // the file pointer and decref the old
            // file, if any
            // job 在新的 binlog 中出现
            filermjob(j->file, j);
            fileaddjob(f, j);
        }
        j->walused += sz;
        f->w->alive += sz;

        return 1;
    case Invalid:
        if (j) {
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    }

Error:
    *err = 1;
    if (j) {
        job_list_remove(j);
        filermjob(j->file, j);
        job_free(j);
    }
    return 0;
}


// Readrec5 is like readrec, but it reads a record in "version 5"
// of the log format.
// 读取一条完整的 job rec 或 full job 到 list l 尾部
static int
readrec5(File *f, Job *l, int *err)
{
    int r, sz = 0;
    size_t namelen;
    Jobrec5 jr;
    Job *j;
    Tube *t;
    char tubename[MAX_TUBE_NAME_LEN];

    // 1. 8 bytes: 读 tube name 长度
    r = read(f->fd, &namelen, sizeof(namelen));
    if (r == -1) {
        twarn("read");
        warnpos(f, 0, "error");
        *err = 1;
        return 0;
    }
    if (r != sizeof(namelen)) {
        return 0;
    }
    sz += r;
    if (namelen >= MAX_TUBE_NAME_LEN) {
        warnpos(f, -r, "namelen %zu exceeds maximum of %d", namelen, MAX_TUBE_NAME_LEN - 1);
        *err = 1;
        return 0;
    }

    if (namelen) {
        // 2. namelen bytes: 读 tube name
        r = readfull(f, tubename, namelen, err, "v5 tube name");
        if (!r) {
            return 0;
        }
        sz += r;
    }
    tubename[namelen] = '\0';

    // 3. 读取完整的 job record
    r = readfull(f, &jr, Jobrec5size, err, "v5 job struct");
    if (!r) {
        return 0;
    }
    sz += r;

    // are we reading trailing zeroes?
    if (!jr.id) return 0;

    j = job_find(jr.id);
    if (!(j || namelen)) {
        // We read a short record without having seen a
        // full record for this job, so the full record
        // was in an eariler file that has been deleted.
        // Therefore the job itself has either been
        // deleted or migrated; either way, this record
        // should be ignored.
        // 没有读到 tube name，此日志非 PUT 写入
        // 但全局 jobs 里没找到，说明此 job 的源 job 所在日志已被删除，本条日志可被忽略
        return 1;
    }

    // 回放状态
    switch (jr.state) {
    case Reserved:
        jr.state = Ready;
        /* Falls through */
    case Ready:
    case Buried:
    case Delayed:
        if (!j) {
            if ((size_t)jr.body_size > job_data_size_limit) {
                warnpos(f, -r, "job %"PRIu64" is too big (%"PRId32" > %zu)",
                        jr.id,
                        jr.body_size,
                        job_data_size_limit);
                goto Error;
            }
            // 首次出现的 job 需创建
            t = tube_find_or_make(tubename);
            j = make_job_with_id(jr.pri, jr.delay, jr.ttr, jr.body_size,
                                 t, jr.id);
            job_list_reset(j);
        }
        // 更新各种属性
        j->r.id = jr.id;
        j->r.pri = jr.pri;
        j->r.delay = jr.delay * 1000; // us => ns
        j->r.ttr = jr.ttr * 1000; // us => ns
        j->r.body_size = jr.body_size;
        j->r.created_at = jr.created_at * 1000; // us => ns
        j->r.deadline_at = jr.deadline_at * 1000; // us => ns
        j->r.reserve_ct = jr.reserve_ct;
        j->r.timeout_ct = jr.timeout_ct;
        j->r.release_ct = jr.release_ct;
        j->r.bury_ct = jr.bury_ct;
        j->r.kick_ct = jr.kick_ct;
        j->r.state = jr.state;

        // 读到一条完整的 job rec
        job_list_insert(l, j);

        // full record; read the job body
        // 如果是带 body 的则继续读
        if (namelen) {
            if (jr.body_size != j->r.body_size) {
                warnpos(f, -r, "job %"PRIu64" size changed", j->r.id);
                warnpos(f, -r, "was %"PRId32", now %"PRId32, j->r.body_size, jr.body_size);
                goto Error;
            }
            r = readfull(f, j->body, j->r.body_size, err, "v5 job body");
            if (!r) {
                goto Error;
            }
            sz += r;

            // since this is a full record, we can move
            // the file pointer and decref the old
            // file, if any
            // 若 job 已在旧 binlog 中出现过，则释放其引用，让 binlog 尝试有序做 GC
            filermjob(j->file, j);
            // 更新 job 指向新的 binlog
            fileaddjob(f, j);
        }
        j->walused += sz;
        f->w->alive += sz;

        return 1;
    case Invalid:
        if (j) {
            // 被 delete 过的 job 直接删除
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    }

Error:
    *err = 1;
    if (j) {
        job_list_remove(j);
        filermjob(j->file, j);
        job_free(j);
    }
    return 0;
}


// 保证从 f 中读取 n 字节到内存 c 中，desc 是读取动作描述，日志记录用
static int
readfull(File *f, void *c, int n, int *err, char *desc)
{
    int r;

    r = read(f->fd, c, n);
    if (r == -1) {
        twarn("read");
        warnpos(f, 0, "error reading %s", desc);
        *err = 1;
        return 0;
    }
    if (r != n) {
        warnpos(f, -r, "unexpected EOF reading %d bytes (got %d): %s", n, r, desc);
        *err = 1;
        return 0;
    }
    return r;
}

static void
warnpos(File *f, int adj, char *fmt, ...)
{
    int off;
    va_list ap;

    off = lseek(f->fd, 0, SEEK_CUR);
    fprintf(stderr, "%s:%d: ", f->path, off+adj);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}


// Opens f for writing, writes a header, and initializes
// f->free and f->resv.
// Sets f->iswopen if successful.
void
filewopen(File *f)
{
    int fd, r;
    int n;
    int ver = Walver;

    fd = open(f->path, O_WRONLY|O_CREAT, 0400);
    if (fd < 0) {
        twarn("open %s", f->path);
        return;
    }

    r = falloc(fd, f->w->filesize);
    if (r) {
        if (close(fd) == -1)
            twarn("close");
        errno = r;
        twarn("falloc %s", f->path);
        r = unlink(f->path);
        if (r) {
            twarn("unlink %s", f->path);
        }
        return;
    }

    // 打开新 binlog 先写 version
    n = write(fd, &ver, sizeof(int));
    if (n < 0 || (size_t)n < sizeof(int)) {
        twarn("write %s", f->path);
        if (close(fd) == -1)
            twarn("close");
        return;
    }

    f->fd = fd;
    f->iswopen = 1;
    fileincref(f);
    f->free = f->w->filesize - n;
    f->resv = 0;
}


static int
filewrite(File *f, Job *j, void *buf, int len)
{
    int r;

    r = write(f->fd, buf, len);
    if (r != len) {
        twarn("write");
        return 0;
    }

    f->w->resv -= r;
    f->resv -= r;
    j->walresv -= r;
    j->walused += r;
    f->w->alive += r;
    return 1;
}


int
filewrjobshort(File *f, Job *j)
{
    int r, nl;

    nl = 0; // name len 0 indicates short record
    // 此处在 job rec 之前写入 tubelen 为 0，用于标识此 job 是 job rec
    r = filewrite(f, j, &nl, sizeof nl) &&
        filewrite(f, j, &j->r, sizeof j->r);
    if (!r) return 0;

    if (j->r.state == Invalid) {
        filermjob(j->file, j);
    }

    return r;
}


int
filewrjobfull(File *f, Job *j)
{
    int nl;

    fileaddjob(f, j);
    nl = strlen(j->tube->name);
    return
        filewrite(f, j, &nl, sizeof nl) &&
        filewrite(f, j, j->tube->name, nl) &&
        filewrite(f, j, &j->r, sizeof j->r) &&
        filewrite(f, j, j->body, j->r.body_size);
}


void
filewclose(File *f)
{
    if (!f) return;
    if (!f->iswopen) return;
    if (f->free) {
        errno = 0;
        if (ftruncate(f->fd, f->w->filesize - f->free) != 0) {
            twarn("ftruncate");
        }
    }
    if (close(f->fd) == -1)
        twarn("close");
    f->iswopen = 0;
    filedecref(f);
}


int
fileinit(File *f, Wal *w, int n)
{
    f->w = w;
    f->seq = n;
    f->path = fmtalloc("%s/binlog.%d", w->dir, n);
    return !!f->path;
}


// Adds f to the linked list in w,
// updating w->tail and w->head as necessary.
// 将 binlog.x file f 追加到链表
Wal*
fileadd(File *f, Wal *w)
{
    if (w->tail) {
        w->tail->next = f;
    }
    w->tail = f;
    if (!w->head) {
        w->head = f;
    }
    w->nfile++;
    return w;
}
