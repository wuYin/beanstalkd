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

static int
grow(Ms *a)
{
    void **nitems;
    size_t ncap = a->cap << 1; // NOTE: grow*2
    if (!ncap)
        ncap = 1;

    nitems = malloc(ncap * sizeof(void *));
    if (!nitems)
        return 0;

    memcpy(nitems, a->items, a->len * sizeof(void *)); // NOTE: copy and replace
    free(a->items);
    a->items = nitems;
    a->cap = ncap;
    return 1;
}

// NOTE: append new item into ms, grow double memory if len reach cap
int
ms_append(Ms *a, void *item)
{
    if (a->len >= a->cap && !grow(a))
        return 0;

    a->items[a->len++] = item;
    if (a->oninsert)
        a->oninsert(a, item, a->len - 1);
    return 1;
}

// NOTE: delete replace last elem with index i elem
// NOTE: lazy delete overwrite, not free memory
static int
ms_delete(Ms *a, size_t i)
{
    void *item;

    if (i >= a->len)
        return 0;
    item = a->items[i];
    a->items[i] = a->items[--a->len]; // NOTE: lazy delete by replaced index i element with last element

    /* it has already been removed now */
    if (a->onremove)
        a->onremove(a, item, i);
    return 1;
}

// NOTE: delete all element and re-init
void
ms_clear(Ms *a)
{
    while (ms_delete(a, 0));
    free(a->items);
    ms_init(a, a->oninsert, a->onremove);
}

// NOTE: find item and delete it
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

// NOTE: traverse entire ms and compare, O(N)
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

// NOTE: sequential take last accessed elem from ms and delete it
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
    ++a->last; // NOTE: incr last itself
    return item;
}
