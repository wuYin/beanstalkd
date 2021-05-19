#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// 全局 tube 集合
struct Ms tubes;

Tube *
make_tube(const char *name)
{
    Tube *t = new(Tube);
    if (!t)
        return NULL;

    // 截断 tube 名字
    strncpy(t->name, name, MAX_TUBE_NAME_LEN);
    if (t->name[MAX_TUBE_NAME_LEN - 1] != '\0') {
        t->name[MAX_TUBE_NAME_LEN - 1] = '\0';
        twarnx("truncating tube name");
    }

    t->ready.less = job_pri_less;
    t->delay.less = job_delay_less;
    t->ready.setpos = job_setpos;
    t->delay.setpos = job_setpos;

    // 头尾指向自身的 dummy 节点
    Job j = {.tube = NULL};
    t->buried = j;
    t->buried.prev = t->buried.next = &t->buried;
    ms_init(&t->waiting_conns, NULL, NULL); // 按顺序 FIFO

    return t;
}

static void
tube_free(Tube *t)
{
    ms_remove(&tubes, t);
    free(t->ready.data);
    free(t->delay.data);
    ms_clear(&t->waiting_conns);
    free(t);
}

// 递减计数若减到 0 则释放 tube
void
tube_dref(Tube *t)
{
    if (!t) return;
    if (t->refs < 1) {
        twarnx("refs is zero for tube: %s", t->name);
        return;
    }

//    fprintf(stderr, "tube: %s, ref:--%d\n", t->name, t->refs);
    --t->refs;
    if (t->refs < 1) {
//        fprintf(stderr, "GCed: %s\n", t->name);
        tube_free(t);
    } // 自动 gc
}

void
tube_iref(Tube *t)
{
    if (!t) return;
//    fprintf(stderr, "tube: %s, ref:++%d\n", t->name, t->refs);
    ++t->refs;
}

static Tube *
make_and_insert_tube(const char *name)
{
    int r;
    Tube *t = NULL;

    t = make_tube(name);
    if (!t)
        return NULL;

    /* We want this global tube list to behave like "weak" refs, so don't
     * increment the ref count. */
    // 全局 tube 列表对 tube 的记录不计数
    r = ms_append(&tubes, t);
    if (!r)
        return tube_dref(t), (Tube *) 0;

    return t;
}

// 遍历全局 tube 集合查找同名 tube
Tube *
tube_find(const char *name)
{
    size_t i;

    for (i = 0; i < tubes.len; i++) {
        Tube *t = tubes.items[i];
        if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0)
            return t;
    }
    return NULL;
}

Tube *
tube_find_or_make(const char *name)
{
    Tube *t = tube_find(name);
    if (t)
        return t;
    return make_and_insert_tube(name);
}

