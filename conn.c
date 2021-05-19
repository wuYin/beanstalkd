#include "dat.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SAFETY_MARGIN (1000000000) /* 1 second */

static int cur_conn_ct = 0, cur_worker_ct = 0, cur_producer_ct = 0;
static uint tot_conn_ct = 0;
int verbose = 0;

// 1. 新建连接 newConn 时，watch default_tube
// 2. OP_WATCH 加入 watch 集合
static void
on_watch(Ms *a, Tube *t, size_t i)
{
    UNUSED_PARAMETER(a);
    UNUSED_PARAMETER(i);
    tube_iref(t); // conn --watch--> tube
    t->watching_ct++;
}

// 1. OP_IGNORE 的 tube
static void
on_ignore(Ms *a, Tube *t, size_t i)
{
    UNUSED_PARAMETER(a);
    UNUSED_PARAMETER(i);
    t->watching_ct--;
    tube_dref(t);
}

Conn *
make_conn(int fd, char start_state, Tube *use, Tube *watch)
{
    Conn *c = new(Conn);
    if (!c) {
        twarn("OOM");
        return NULL;
    }

    // 将 tubeX 加入 watch 集合
    ms_init(&c->watch, (ms_event_fn) on_watch, (ms_event_fn) on_ignore);

    if (!ms_append(&c->watch, watch)) {
        free(c);
        twarn("OOM");
        return NULL;
    }

    // 2. use default tube, default ref++
    TUBE_ASSIGN(c->use, use);
    use->using_ct++;

    // 配置字段
    c->sock.fd = fd;
    c->state = start_state;
    c->pending_timeout = -1; // 无限 reserve 等待
    c->tickpos = 0; // Does not mean anything if in_conns is set to 0.
    c->in_conns = 0;

    // The list is empty.
    job_list_reset(&c->reserved_jobs);

    /* stats */
    cur_conn_ct++;
    tot_conn_ct++;

    return c;
}

void
connsetproducer(Conn *c)
{
    if (c->type & CONN_TYPE_PRODUCER) return;
    c->type |= CONN_TYPE_PRODUCER;
    cur_producer_ct++; /* stats */
}

void
connsetworker(Conn *c)
{
    if (c->type & CONN_TYPE_WORKER) return;
    c->type |= CONN_TYPE_WORKER;
    cur_worker_ct++; /* stats */
}

int
count_cur_conns()
{
    return cur_conn_ct;
}

uint
count_tot_conns()
{
    return tot_conn_ct;
}

int
count_cur_producers()
{
    return cur_producer_ct;
}

int
count_cur_workers()
{
    return cur_worker_ct;
}

static int
has_reserved_job(Conn *c)
{
    return !job_list_is_empty(&c->reserved_jobs);
}


// Returns positive nanoseconds when c should tick, 0 otherwise.
// 计算下次超时事件的时间戳
// conn 有 2 种超时事件：reserved TTR 到期、reserve 等待超时
static int64
conntickat(Conn *c)
{
    int margin = 0, should_timeout = 0;
    int64 t = INT64_MAX;

    // conn 还在 reserve 则有 margin 安全时间
    if (conn_waiting(c)) {
        margin = SAFETY_MARGIN;
    }

    // 1. 最小 reserved TTR 到期时间
    if (has_reserved_job(c)) {
        t = connsoonestjob(c)->r.deadline_at - nanoseconds() - margin;
        should_timeout = 1;
    }
    // 2. reserve 等待超时
    if (c->pending_timeout >= 0) {
        t = min(t, ((int64)c->pending_timeout) * 1000000000);
        should_timeout = 1;
    }

    if (should_timeout) {
        return nanoseconds() + t;
    }
    return 0;
}


// Remove c from the c->srv heap and reschedule it using the value
// returned by conntickat if there is an outstanding timeout in the c.
// 刷新 c 的 reserve-timeout / 最快 TTR 时间
void
connsched(Conn *c)
{
    // 1. 将 c 从 server 的 conns 堆中移除
    if (c->in_conns) {
        heapremove(&c->srv->conns, c->tickpos);
        c->in_conns = 0;
    }
    // 2. 刷新下次超时时间戳，重新调度
    c->tickat = conntickat(c);

    // 3. 无超时则不加入 s.conns 队列中（如从未 reserve）
    if (c->tickat) {
        heapinsert(&c->srv->conns, c);
        c->in_conns = 1;
    }
}

// conn_set_soonestjob updates c->soonest_job with j
// if j should be handled sooner than c->soonest_job.
static void
conn_set_soonestjob(Conn *c, Job *j) {
    if (!c->soonest_job || j->r.deadline_at < c->soonest_job->r.deadline_at) {
        c->soonest_job = j;
    }
}

// Return the reserved job with the earliest deadline,
// or NULL if there's no reserved job.
// 遍历 conn 的 reserved 链表，找出 job.deadline_at 最小的 job，即 TTR 最快超时的 job
Job *
connsoonestjob(Conn *c)
{
    // use cached value and bail out.
    if (c->soonest_job != NULL)
        return c->soonest_job;

    Job *j = NULL;
    for (j = c->reserved_jobs.next; j != &c->reserved_jobs; j = j->next) {
        conn_set_soonestjob(c, j);
    }
    return c->soonest_job;
}

// 记录本次 reserve，更新 job 状态
void
conn_reserve_job(Conn *c, Job *j) {
    j->tube->stat.reserved_ct++;
    j->r.reserve_ct++;

    j->r.deadline_at = nanoseconds() + j->r.ttr;
    j->r.state = Reserved;
    job_list_insert(&c->reserved_jobs, j);
    j->reserver = c;
    c->pending_timeout = -1;
    // 根据新到期 ttr 更新 soonestjob 字段
    conn_set_soonestjob(c, j);
}

// Return true if c has a reserved job with less than one second until its
// deadline.
int
conndeadlinesoon(Conn *c)
{
    int64 t = nanoseconds();
    Job *j = connsoonestjob(c);

    // reserved 超时时间进入 TTR safe margin
    return j && t >= j->r.deadline_at - SAFETY_MARGIN;
}

int
conn_ready(Conn *c)
{
    size_t i;

    for (i = 0; i < c->watch.len; i++) {
        if (((Tube *) c->watch.items[i])->ready.len)
            return 1;
    }
    return 0;
}


int
conn_less(void *ca, void *cb)
{
    Conn *a = (Conn *)ca;
    Conn *b = (Conn *)cb;
    return a->tickat < b->tickat;
}


void
conn_setpos(void *c, size_t i)
{
    ((Conn *)c)->tickpos = i;
}


void
connclose(Conn *c)
{
    sockwant(&c->sock, 0);
    close(c->sock.fd);
    if (verbose) {
        printf("close %d\n", c->sock.fd);
    }

    job_free(c->in_job);

    /* was this a peek or stats command? */
    if (c->out_job && c->out_job->r.state == Copy)
        job_free(c->out_job);

    c->in_job = c->out_job = NULL;
    c->in_job_read = 0;

    if (c->type & CONN_TYPE_PRODUCER) cur_producer_ct--; /* stats */
    if (c->type & CONN_TYPE_WORKER) cur_worker_ct--; /* stats */

    cur_conn_ct--; /* stats */

    remove_waiting_conn(c);
    if (has_reserved_job(c))
        enqueue_reserved_jobs(c);

    ms_clear(&c->watch);
    c->use->using_ct--;
    // 3. 连接关闭时，c.use--
    TUBE_ASSIGN(c->use, NULL);

    if (c->in_conns) {
        heapremove(&c->srv->conns, c->tickpos);
        c->in_conns = 0;
    }

    free(c);
}
