#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <limits.h>

static int reserve(Wal *w, int n);


// Reads w->dir for files matching binlog.NNN,
// sets w->next to the next unused number, and
// returns the minimum number.
// If no files are found, sets w->next to 1 and
// returns a large number.
// 扫描 w->dir 日志目录，获取 binlog.NNN 后缀，返回下一个未使用的 binlog 序号，并递增 w->next
// 返回 min，而 w->next 为 max+1
static int
walscandir(Wal *w)
{
    static char base[] = "binlog.";
    static const int len = sizeof(base) - 1;
    DIR *d;
    struct dirent *e;
    int min = 1<<30; // 类似 INT_MAX
    int max = 0;
    int n;
    char *p;

    d = opendir(w->dir);
    if (!d) return min; // binlog 目录不存在

    // 逐个扫描
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, base, len) == 0) {
            n = strtol(e->d_name+len, &p, 10);
            if (p && *p == '\0') {
                if (n > max) max = n;
                if (n < min) min = n;
            }
        }
    }

    closedir(d);
    w->next = max + 1; // 递增
    return min;
}


// 只要有 binlog file 引用计数归零，就尝试做一次 GC
// 注意只是尝试，有不触发的可能
// 因为设计上，GC 顺序是严格从 w.head 到 w.tail 的
void
walgc(Wal *w)
{
    File *f;

    while (w->head && !w->head->refs) {
        f = w->head;
        w->head = f->next;
        if (w->tail == f) {
            w->tail = f->next; // also, f->next == NULL
        }

        w->nfile--;
        unlink(f->path);
        free(f->path);
        free(f);
    }
}


// returns 1 on success, 0 on error.
static int
usenext(Wal *w)
{
    File *f;

    f = w->cur;
    if (!f->next) {
        twarnx("there is no next wal file");
        return 0;
    }

    w->cur = f->next;
    filewclose(f);
    return 1;
}


static int
ratio(Wal *w)
{
    int64 n, d;

    d = w->alive + w->resv; // 写入 file 的字节数，加上预留字节数
    n = (int64)w->nfile * (int64)w->filesize - d; // 所有 binlog 大小上限 - 已使用了容量 = 未使用的
    if (!d) return 0;
    return n / d;
}


// Returns the number of bytes reserved or 0 on error.
static int
walresvmigrate(Wal *w, Job *j)
{
    int z = 0;

    // reserve only space for the migrated full job record
    // space for the delete is already reserved
    z += sizeof(int);
    z += strlen(j->tube->name);
    z += sizeof(Jobrec);
    z += j->r.body_size;

    return reserve(w, z);
}


static void
moveone(Wal *w)
{
    Job *j;

    if (w->head == w->cur || w->head->next == w->cur) {
        // no point in moving a job
        return;
    }

    j = w->head->jlist.fnext;
    if (!j || j == &w->head->jlist) {
        // head holds no jlist; can't happen
        twarnx("head holds no jlist");
        return;
    }

    if (!walresvmigrate(w, j)) {
        // it will not fit, so we'll try again later
        return;
    }

    filermjob(w->head, j);
    w->nmig++;
    walwrite(w, j);
}


static void
walcompact(Wal *w)
{
    int r;

    // idle / used >= 2 // idle >= 2*used
    for (r=ratio(w); r>=2; r--) {
        // 一个个 Job 地尝试做 compact
        moveone(w);
    }
}


static void
walsync(Wal *w)
{
    int64 now;

    now = nanoseconds();
    if (w->wantsync && now >= w->lastsync+w->syncrate) {
        w->lastsync = now;
        if (fsync(w->cur->fd) == -1) {
            twarn("fsync");
        }
    }
}


// Walwrite writes j to the log w (if w is enabled).
// On failure, walwrite disables w and returns 0; on success, it returns 1.
// Unlke walresv*, walwrite should never fail because of a full disk.
// If w is disabled, then walwrite takes no action and returns 1.
// 将 j 写入 binlog
int
walwrite(Wal *w, Job *j)
{
    int r = 0;

    if (!w->use) return 1;
    if (w->cur->resv > 0 || usenext(w)) {
        if (j->file) {
            // 只写 job rec
            r = filewrjobshort(w->cur, j);
        } else {
            // 还要写 job body
            r = filewrjobfull(w->cur, j);
        }
    }
    if (!r) {
        filewclose(w->cur);
        w->use = 0;
    }
    w->nrec++;
    return r;
}


void
walmaint(Wal *w)
{
    if (w->use) {
        walcompact(w);
        walsync(w);
    }
}


static int
makenextfile(Wal *w)
{
    File *f;

    f = new(File);
    if (!f) {
        twarnx("OOM");
        return 0;
    }

    if (!fileinit(f, w, w->next)) {
        free(f);
        twarnx("OOM");
        return 0;
    }

    filewopen(f);
    if (!f->iswopen) {
        free(f->path);
        free(f);
        return 0;
    }

    w->next++;
    fileadd(f, w);
    return 1;
}


static void
moveresv(File *to, File *from, int n)
{
    from->resv -= n;
    from->free += n;
    to->resv += n;
    to->free -= n;
}


static int
needfree(Wal *w, int n)
{
    if (w->tail->free >= n) return n;
    // 容量不足，滚动到下一个 binlog
    if (makenextfile(w)) return n;
    return 0;
}


// Ensures:
//  1. b->resv is congruent to n (mod z).
//  2. x->resv is congruent to 0 (mod z) for each future file x.
// Assumes (and preserves) that b->resv >= n.
// Reserved space is conserved (neither created nor destroyed);
// we just move it around to preserve the invariant.
// We might have to allocate a new file.
// Returns 1 on success, otherwise 0. If there was a failure,
// w->tail is not updated.
static int
balancerest(Wal *w, File *b, int n)
{
    int rest, c, r;
    static const int z = sizeof(int) + sizeof(Jobrec);

    if (!b) return 1;

    rest = b->resv - n;
    r = rest % z;
    if (r == 0) return balancerest(w, b->next, 0);

    c = z - r;
    if (w->tail->resv >= c && b->free >= c) {
        moveresv(b, w->tail, c);
        return balancerest(w, b->next, 0);
    }

    if (needfree(w, r) != r) {
        twarnx("needfree");
        return 0;
    }
    moveresv(w->tail, b, r);
    return balancerest(w, b->next, 0);
}


// Ensures:
//  1. w->cur->resv >= n.
//  2. w->cur->resv is congruent to n (mod z).
//  3. x->resv is congruent to 0 (mod z) for each future file x.
// (where z is the size of a delete record in the wal).
// Reserved space is conserved (neither created nor destroyed);
// we just move it around to preserve the invariant.
// We might have to allocate a new file.
// Returns 1 on success, otherwise 0. If there was a failure,
// w->tail is not updated.
// 将空间不足的 cur binlog 滚动 n 字节到 tail binlog
// 挪动 cur 到 next binlog 并关闭
static int
balance(Wal *w, int n)
{
    // Invariant 1
    // (this loop will run at most once)
    while (w->cur->resv < n) {
        int m = w->cur->resv;

        int r = needfree(w, m);
        if (r != m) {
            twarnx("needfree");
            return 0;
        }

        moveresv(w->tail, w->cur, m);
        usenext(w);
    }

    // Invariants 2 and 3
    return balancerest(w, w->cur, n);
}


// Returns the number of bytes successfully reserved: either 0 or n.
// reserve 是 walwrite 的前置操作，二者其实是绑定的，不会出现无限 reserve 的情况
// 空间不足则滚动 binlog
static int
reserve(Wal *w, int n)
{
    int r;

    // return value must be nonzero but is otherwise ignored
    if (!w->use) return 1;

    if (w->cur->free >= n) {
        w->cur->free -= n;
        w->cur->resv += n;
        w->resv += n;
        return n;
    }

    // 当前 binlog 剩余空间不足，创建下一个 binlog 更新 cur 使用并关闭之前的 cur binlog
    r = needfree(w, n);
    if (r != n) {
        twarnx("needfree");
        return 0;
    }

    w->tail->free -= n;
    w->tail->resv += n;
    w->resv += n;
    if (!balance(w, n)) {
        // error; undo the reservation
        w->resv -= n;
        w->tail->resv -= n;
        w->tail->free += n;
        return 0;
    }

    return n;
}


// Returns the number of bytes reserved or 0 on error.
// 为 put 新 job j 预留/扩展空间
int
walresvput(Wal *w, Job *j)
{
    int z = 0;

    // reserve space for the initial job record
    // job_line
    z += sizeof(int); // 1. tube_name_len
    z += strlen(j->tube->name); // 2. tube_name
    z += sizeof(Jobrec); // 3. job_rec
    z += j->r.body_size; // 4. job_body

    // plus space for a delete to come later
    z += sizeof(int); // 预留之后此 job 的其他操作
    z += sizeof(Jobrec);

    return reserve(w, z);
}


// Returns the number of bytes reserved or 0 on error.
// 为更新 job 的新状态预留空间
int
walresvupdate(Wal *w)
{
    int z = 0;
    z +=sizeof(int);
    z +=sizeof(Jobrec);
    return reserve(w, z);
}


// Returns the number of locks acquired: either 0 or 1.
// 打开 bindir/lock 文件并上锁，故意丢弃 fd
int
waldirlock(Wal *w)
{
    int r;
    int fd;
    struct flock lk;
    char *path;
    size_t path_length;

    path_length = strlen(w->dir) + strlen("/lock") + 1;
    if ((path = malloc(path_length)) == NULL) {
        twarn("malloc");
        return 0;
    }
    snprintf(path, path_length, "%s/lock", w->dir);

    fd = open(path, O_WRONLY|O_CREAT, 0600);
    free(path);
    if (fd == -1) {
        twarn("open");
        return 0;
    }

    lk.l_type = F_WRLCK;
    lk.l_whence = SEEK_SET;
    lk.l_start = 0;
    lk.l_len = 0;
    r = fcntl(fd, F_SETLK, &lk);
    if (r) {
        twarn("fcntl");
        return 0;
    }

    // intentionally leak fd, since we never want to close it
    // and we'll never need it again
    // 不释放 wal lock 的 fd
    return 1;
}


void
walread(Wal *w, Job *list, int min)
{
    int i;
    int err = 0;

    // 回放所有 binlog 文件 [min, max/w->next-1]
    for (i = min; i < w->next; i++) {
        File *f = new(File);
        if (!f) {
            twarnx("OOM");
            exit(1);
        }

        if (!fileinit(f, w, i)) {
            free(f);
            twarnx("OOM");
            exit(1);
        }

        int fd = open(f->path, O_RDONLY);
        if (fd < 0) {
            twarn("open %s", f->path);
            free(f->path);
            free(f);
            continue;
        }

        f->fd = fd;
        // 记录此 binlog
        fileadd(f, w);
        // 回放 binlog job 到 list
        err |= fileread(f, list);
        if (close(fd) == -1)
            twarn("close");
    }

    if (err) {
        warnx("Errors reading one or more WAL files.");
        warnx("Continuing. You may be missing data.");
    }
}


void
walinit(Wal *w, Job *list)
{
    int min;

    // 扫描获取 min, next binlog 序号
    min = walscandir(w);
    // 回放所有 binlog
    walread(w, list, min);

    // first writable file
    // 创建下一个 binlog file
    if (!makenextfile(w)) {
        twarnx("makenextfile");
        exit(1);
    }

    // 当前使用最后一个
    w->cur = w->tail;
}
