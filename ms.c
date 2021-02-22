#include "dat.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

void
ms_init(Ms *a, ms_event_fn oninsert, ms_event_fn onremove)
{
    a->len = a->cap = a->last = 0;
    a->items = NULL;
    a->oninsert = oninsert;
    a->onremove = onremove;
}

// 动态扩容
static int
grow(Ms *a)
{
    void **nitems;
    size_t ncap = a->cap << 1; // <<1 即 *2
    if (!ncap)
        ncap = 1;

    nitems = malloc(ncap * sizeof(void *));
    if (!nitems)
        return 0;

    memcpy(nitems, a->items, a->len * sizeof(void *)); // 内存拷贝
    free(a->items);
    a->items = nitems;
    a->cap = ncap;
    return 1;
}

int
ms_append(Ms *a, void *item)
{
    // 容量不足则扩容
    if (a->len >= a->cap && !grow(a))
        return 0;

    a->items[a->len++] = item;
    if (a->oninsert)
        a->oninsert(a, item, a->len - 1);
    return 1;
}

static int
ms_delete(Ms *a, size_t i)
{
    void *item;

    if (i >= a->len)
        return 0;
    item = a->items[i];
    a->items[i] = a->items[--a->len]; // 仅把 i 重新指向了 last 并递减 len，完全依靠 len 作为数据有效性的边界

    /* it has already been removed now */
    if (a->onremove)
        a->onremove(a, item, i);
    return 1;
}

void
ms_clear(Ms *a)
{
    while (ms_delete(a, 0));
    free(a->items);
    ms_init(a, a->oninsert, a->onremove);
}

// 查找集合并删除
int
ms_remove(Ms *a, void *item)
{
    size_t i;

    for (i = 0; i < a->len; i++) {
        if (a->items[i] == item)
            return ms_delete(a, i);
    }
    return 0;
}

// 遍历查找
int
ms_contains(Ms *a, void *item)
{
    size_t i;

    for (i = 0; i < a->len; i++) {
        if (a->items[i] == item)
            return 1;
    }
    return 0;
}

// 有序读取下一个元素（也是最旧的），并删除
void *
ms_take(Ms *a)
{
    void *item;

    if (!a->len)
        return NULL;

    // The result of last behaviour is that ms_take returns the oldest elements
    // first, exception is a row of multiple take calls without inserts on ms
    // of even number of elements. See the test.
    a->last = a->last % a->len;
    item = a->items[a->last];
    ms_delete(a, a->last);
    ++a->last; // last 持续递增
    return item;
}
