#include <stdint.h>
#include <stdlib.h>

typedef unsigned char uchar;
typedef uchar         byte;
typedef unsigned int  uint;
typedef int32_t       int32;
typedef uint32_t      uint32;
typedef int64_t       int64;
typedef uint64_t      uint64;

typedef struct Ms     Ms; // 动态扩容的无序集合
typedef struct Job    Job;
typedef struct Tube   Tube;
typedef struct Conn   Conn;
typedef struct Heap   Heap; // NOTE: min heap
typedef struct Jobrec Jobrec;
typedef struct File   File;
typedef struct Socket Socket;
typedef struct Server Server;
typedef struct Wal    Wal;

typedef void(*Handle)(void*, int rw);
typedef int(FAlloc)(int, int);


// NUM_PRIMES is used in the jobs hashing.
#if _LP64
#define NUM_PRIMES 48
#else
#define NUM_PRIMES 19
#endif

/* Some compilers (e.g. gcc on SmartOS) define NULL as 0.
 * This is allowed by the C standard, but is unhelpful when
 * using NULL in most pointer contexts with errors turned on. */
#if (defined(sun) || defined(__sun)) && (defined(__SVR4) || defined(__svr4__))
#ifdef NULL
#undef NULL
#endif
#define NULL ((void*)0)
#endif

// The name of a tube cannot be longer than MAX_TUBE_NAME_LEN-1
#define MAX_TUBE_NAME_LEN 201

// A command can be at most LINE_BUF_SIZE chars, including "\r\n". This value
// MUST be enough to hold the longest possible command ("pause-tube a{200} 4294967295\r\n")
// or reply line ("USING a{200}\r\n").
// 11 为最长 cmd： `pause-tube `
// 12 为最长参数：` 4294967295`
#define LINE_BUF_SIZE (11 + MAX_TUBE_NAME_LEN + 12)

#define min(a,b) ((a)<(b)?(a):(b))

// Jobs with priority less than URGENT_THRESHOLD are counted as urgent.
#define URGENT_THRESHOLD 1024

// The default maximum job size. // 64KB
#define JOB_DATA_SIZE_LIMIT_DEFAULT ((1 << 16) - 1)

// The maximum value that job_data_size_limit can be set to via "-z".
// It could be up to INT32_MAX-2 (~2GB), but set it to 1024^3 (1GB).
// The width is restricted by Jobrec.body_size that is int32.
#define JOB_DATA_SIZE_LIMIT_MAX 1073741824

// The default value for the fsync (-f) parameter, milliseconds.
#define DEFAULT_FSYNC_MS 50

// Use this macro to designate unused parameters in functions.
#define UNUSED_PARAMETER(x) (void)(x)

// version is defined in vers.c, see vers.sh for details.
extern const char version[];

// verbose holds the count of -V parameters; it's a verbosity level.
extern int verbose;

extern struct Server srv;

// Replaced by tests to simulate failures.
extern FAlloc *falloc;

// stats structure holds counters for operations, both globally and per tube.
struct stats {
    uint64 urgent_ct; // NOTE: urgent jobs which priority < 1024
    uint64 waiting_ct; // NOTE: reserving conns
    uint64 buried_ct;
    uint64 reserved_ct;
    uint64 pause_ct; // NOTE: pause-tube exec counts
    uint64 total_delete_ct;
    uint64 total_jobs_ct;
};


// less_fn is used by the binary heap to determine the order of elements.
typedef int(*less_fn)(void*, void*);

// setpos_fn is used by the binary heap to record the new positions of elements
// whenever they get moved or inserted.
// NOTE: calculate new pos for new value
typedef void(*setpos_fn)(void*, size_t);

struct Heap {
    size_t  cap;                // capacity of the heap
    size_t  len;                // amount of elements in the heap // 从 0 开始计数，也可作为 next 索引
    void    **data;             // actual elements

    less_fn   less; // 函数指针自定义堆元素比较逻辑
    setpos_fn setpos; // 位置更新的回调
};
int   heapinsert(Heap *h, void *x);
void* heapremove(Heap *h, size_t k);


// attach 到 epoll 事件的 data.ptr 上，方便调用 handler
struct Socket {
    // Descriptor for the socket.
    int    fd;

    // f can point to srvaccept or prothandle.
    Handle f;

    // x is passed as first parameter to f.
    void   *x;

    // added value is platform depended: on OSX it can be > 1.
    // Value of 1 - socket was already added to event notifications,
    // otherwise it is 0.
    // 是否为第一次注册
    int    added;
};

int sockinit(void);

// sockwant updates event filter for the socket s. rw designates
// the kind of event we should be notified about:
// 'r' - read
// 'w' - write
// 'h' - hangup (closed connection)
// 0   - ignore this socket
int sockwant(Socket *s, int rw);

// socknext waits for the next event at most timeout nanoseconds.
// If event happens before timeout then s points to the corresponding socket,
// and the kind of event is returned. In case of timeout, 0 is returned.
int socknext(Socket **s, int64 timeout);


// ms_event_fn is called with the element being inserted/removed and its position.
typedef void(*ms_event_fn)(Ms *a, void *item, size_t i);

// Resizable multiset
struct Ms {
    size_t len;                // amount of stored elements
    size_t cap;                // capacity
    size_t last;               // position of last taken element
    void **items;

    ms_event_fn oninsert;      // called on insertion of an element
    ms_event_fn onremove;      // called on removal of an element
};

void ms_init(Ms *a, ms_event_fn oninsert, ms_event_fn onremove);
void ms_clear(Ms *a);
int ms_append(Ms *a, void *item);
int ms_remove(Ms *a, void *item);
int ms_contains(Ms *a, void *item);
void *ms_take(Ms *a);


enum // Jobrec.state
{
    Invalid,
    Ready,
    Reserved,
    Buried,
    Delayed,
    Copy
};

enum
{
    Walver = 7
};

// If you modify Jobrec struct, you must increment Walver above.
//
// This workflow is expected:
// 1. If any change needs to be made to the format, first increment Walver.
// 2. If and only if this is the first such change since the last release:
//    a. Copy-paste relevant file-reading functions in file.c and
//       add the old version number to their names. For example,
//       if you are incrementing Walver from 7 to 8, copy readrec to readrec7.
//       (Currently, there is only one such function, readrec. But if
//       a future readrec calls other version-specific functions,
//       those will have to be copied too.)
// 3. Add a switch case to fileread for the old version.
// 4. Modify the current reading function (readrec) to reflect your change.
//
// Incrementing Walver for every change, even if not every version
// will be released, is helpful even if it "wastes" version numbers.
// It is a really easy thing to do and it means during development
// you won't have to worry about misinterpreting the contents of a binlog
// that you generated with a dev copy of beanstalkd.

struct Jobrec {
    uint64 id;
    uint32 pri;
    int64  delay;
    int64  ttr;
    int32  body_size;
    int64  created_at; // 服务端创建 job 时间

    // deadline_at is a timestamp, in nsec, that points to:
    // * undefined otherwise.
    // 1. delay 到期时间戳，delay 到 ready
    // 2. TTR 到期时间戳，reserved 到 ready
    int64  deadline_at;

    uint32 reserve_ct;
    uint32 timeout_ct;
    uint32 release_ct;
    uint32 bury_ct;
    uint32 kick_ct;
    byte   state;
};

struct Job {
     // persistent fields; these get written to the wal
    Jobrec r;

    // bookkeeping fields; these are in-memory only
    char pad[6];
    Tube *tube;                 // NOTE: tube which job belong to
    Job *prev, *next;           // linked list of jobs // 用于连接 buried 或 reserved jobs 链表
    Job *ht_next;               // Next job in a hash table list // NOTE: using for tube heap linked list
    size_t heap_index;          // where is this job in its current heap // NOTE: index of it's tube heap
    File *file;
    Job  *fnext;
    Job  *fprev;
    void *reserver; // job 只能被 reserver delete/touch
    int walresv;
    int walused;

    char *body;                 // written separately to the wal // ASCII job body
};

struct Tube {
    uint refs;        // reference count
    char name[MAX_TUBE_NAME_LEN];
    Heap ready;
    Heap delay;       // 倒计时毫秒数
    Job buried;       // 链表头

    Ms waiting_conns; // 等待 Job 的消费连接

    struct stats stat;
    uint using_ct;
    uint watching_ct;

    int64 pause;      // 是否被暂停
    int64 unpause_at; // 恢复时间戳 ns
};


// Prints warning message on stderr in the format:
// <progname>: FILE:LINE in FUNC: <fmt>: <errno_msg>
#define twarn(...) __twarn(__VA_ARGS__, "")

// Hack to quiet the compiler. When VA_ARGS in twarn() has one element,
// e.g. twarn("OOM"), its replaced with __twarn("OOM", ""),
// thus VA_ARGS is expanded to at least one element in warn().
#define __twarn(fmt, ...) \
    warn("%s:%d in %s: " fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__)

// Prints warning message on stderr in the format:
// <progname>: FILE:LINE in FUNC: <fmt>
#define twarnx(...) __twarnx(__VA_ARGS__, "")

// See __twarn macro.
#define __twarnx(fmt, ...) \
    warnx("%s:%d in %s: " fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__)

void warn(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void warnx(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
char* fmtalloc(char *fmt, ...) __attribute__((format(printf, 1, 2)));
void* zalloc(int n);
#define new(T) zalloc(sizeof(T)) // new actually malloc + memset init
void optparse(Server*, char**);

extern const char *progname;

int64 nanoseconds(void);
int   rawfalloc(int fd, int len);

// Take ID for a jobs from next_id and allocate and store the job.
#define make_job(pri,delay,ttr,body_size,tube) \
    make_job_with_id(pri,delay,ttr,body_size,tube,0)

Job *allocate_job(int body_size);
Job *make_job_with_id(uint pri, int64 delay, int64 ttr,
                      int body_size, Tube *tube, uint64 id);
void job_free(Job *j);

/* Lookup a job by job ID */
Job *job_find(uint64 job_id);

/* the void* parameters are really job pointers */
void job_setpos(void *j, size_t pos);
int job_pri_less(void *ja, void *jb);
int job_delay_less(void *ja, void *jb);

Job *job_copy(Job *j);

const char * job_state(Job *j);

void job_list_reset(Job *head);
int job_list_is_empty(Job *head);
Job *job_list_remove(Job *j);
void job_list_insert(Job *head, Job *j);

/* for unit tests */
size_t get_all_jobs_used(void);


extern struct Ms tubes;

Tube *make_tube(const char *name);
void  tube_dref(Tube *t);
void  tube_iref(Tube *t);
Tube *tube_find(const char *name);
Tube *tube_find_or_make(const char *name);

// 更新计数引用的 tube 赋值
#define TUBE_ASSIGN(a,b) (tube_dref(a), (a) = (b), tube_iref(a))


Conn *make_conn(int fd, char start_state, Tube *use, Tube *watch);

int count_cur_conns(void);
uint count_tot_conns(void);
int count_cur_producers(void);
int count_cur_workers(void);


extern size_t primes[];


extern size_t job_data_size_limit;

void prot_init(void);
int64 prottick(Server *s);

void remove_waiting_conn(Conn *c);

void enqueue_reserved_jobs(Conn *c);

void enter_drain_mode(int sig);
void h_accept(const int fd, const short which, Server *s);
int  prot_replay(Server *s, Job *list);


int make_server_socket(char *host, char *port);


// CONN_TYPE_* are bit masks used to track the type of connection.
// A put command adds the PRODUCER type, "reserve*" adds the WORKER type.
// If connection awaits for data, then it has WAITING type.
#define CONN_TYPE_PRODUCER 1
#define CONN_TYPE_WORKER   2
#define CONN_TYPE_WAITING  4

struct Conn {
    Server *srv;
    Socket sock;
    char   state;       // see the STATE_* description
    char   type;        // combination of CONN_TYPE_* values
    Conn   *next;       // only used in epollq functions
    Tube   *use;        // tube currently in use // put
    int64  tickat;      // time at which to do more work; determines pos in heap
    size_t tickpos;     // position in srv->conns, stale when in_conns=0
    byte   in_conns;    // 1 if the conn is in srv->conns heap, 0 otherwise
    Job    *soonest_job;// memoization of the soonest job // 在 reserved jobs 中 TTR 最快到期的 job
    int    rw;          // currently want: 'r', 'w', or 'h'

    // How long client should "wait" for the next job; -1 means forever. // 仅 reserve 为 -1
    int    pending_timeout;

    // Used to inform state machine that client no longer waits for the data.
    char   halfclosed;

    // 读请求 buffer
    char   cmd[LINE_BUF_SIZE];     // this string is NOT NUL-terminated
    size_t cmd_len;
    int    cmd_read;

    // 写请求 buffer
    char *reply;
    int  reply_sent;
    int  reply_len;
    char reply_buf[LINE_BUF_SIZE]; // this string IS NUL-terminated

    // How many bytes of in_job->body have been read so far. If in_job is NULL
    // while in_job_read is nonzero, we are in bit bucket mode and
    // in_job_read's meaning is inverted -- then it counts the bytes that
    // remain to be thrown away.
    // in_job 为 NULL 但 in_job_read 不为 0，说明在 bucket 模式，丢弃读
    Job   *in_job;              // a job to be read from the client // 读 job
    int64 in_job_read;          // 已读或要读的 body 大小

    // NOTE: writing job
    Job *out_job;               // a job to be sent to the client
    int out_job_sent;           // how many bytes of *out_job were sent already

    Ms  watch;                  // the set of watched tubes by the connection // 正在 watch 的 tube 集合
    Job reserved_jobs;          // linked list header // 被 reserved 的 job 的链表
};
int  conn_less(void *ca, void *cb);
void conn_setpos(void *c, size_t i);
void connsched(Conn *c);
void connclose(Conn *c);
void connsetproducer(Conn *c);
void connsetworker(Conn *c);
Job *connsoonestjob(Conn *c);
int  conndeadlinesoon(Conn *c);
int conn_ready(Conn *c);
void conn_reserve_job(Conn *c, Job *j);
#define conn_waiting(c) ((c)->type & CONN_TYPE_WAITING)




enum
{
    Filesizedef = (10 << 20)
};

struct Wal {
    int    filesize; // NOTE: -s max binlog file size
    int    use; // 命令行是否有 -b bin_dir
    char   *dir; // -b binlog dir
    File   *head; // 打开的 binlog 文件链表
    File   *cur;
    File   *tail;
    int    nfile;
    int    next; // 下一 binlog 序号

    // stat
    int64  resv;  // bytes reserved
    int64  alive; // bytes in use

    int64  nmig;  // migrations
    int64  nrec;  // records written ever
    int    wantsync; // do we sync to disk?
    int64  syncrate; // how often we sync to disk, in nanoseconds
    int64  lastsync;
};
int  waldirlock(Wal*);
void walinit(Wal*, Job *list);
int  walwrite(Wal*, Job*);
void walmaint(Wal*);
int  walresvput(Wal*, Job*);
int  walresvupdate(Wal*);
void walgc(Wal*);


struct File {
    File *next; // 供 wal 链表使用
    uint refs;
    int  seq;
    int  iswopen; // is open for writing
    int  fd;
    int  free;
    int  resv;
    char *path; // binlog 路径
    Wal  *w;

    Job jlist;    // jobs written in this file
};
int  fileinit(File*, Wal*, int);
Wal* fileadd(File*, Wal*);
void fileincref(File*);
void filedecref(File*);
void fileaddjob(File*, Job*);
void filermjob(File*, Job*);
int  fileread(File*, Job *list);
void filewopen(File*);
void filewclose(File*);
int  filewrjobshort(File*, Job*);
int  filewrjobfull(File*, Job*);


#define Portdef "11300"

struct Server {
    char *port;
    char *addr;
    char *user;

    Wal    wal;
    Socket sock;

    // Connections that must produce deadline or timeout, ordered by the time.
    // 等待连接按 tickat 倒计时的紧急度为优先级
    Heap   conns;
};
void srv_acquire_wal(Server *s);
void srvserve(Server *s);
void srvaccept(Server *s, int ev);
