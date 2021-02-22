#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


// 更新位置 k 的值为 x
static void
set(Heap *h, size_t k, void *x)
{
    h->data[k] = x;
    h->setpos(x, k);
}


static void
swap(Heap *h, size_t a, size_t b)
{
    void *tmp;

    tmp = h->data[a];
    set(h, a, h->data[b]);
    set(h, b, tmp);
}


static int
less(Heap *h, size_t a, size_t b)
{
    return h->less(h->data[a], h->data[b]);
}


// 向堆顶冒泡
static void
siftdown(Heap *h, size_t k)
{
    for (;;) {
        size_t p = (k-1) / 2; /* parent */

        // 没父节点小则冒泡结束
        if (k == 0 || less(h, p, k)) {
            return;
        }

        // 比父节点小则交换
        swap(h, k, p);
        k = p;
    }
}


// 向堆底下沉
static void
siftup(Heap *h, size_t k)
{
    for (;;) {
        size_t l = k*2 + 1; /* left child */
        size_t r = k*2 + 2; /* right child */

        /* find the smallest of the three */
        size_t s = k;
        if (l < h->len && less(h, l, s)) s = l;
        if (r < h->len && less(h, r, s)) s = r;

        // 左右孩子都比自己大，无法再下沉
        if (s == k) {
            return; /* satisfies the heap property */
        }

        // 与最小的子节点交换
        swap(h, k, s);
        k = s;
    }
}


// Heapinsert inserts x into heap h according to h->less.
// It returns 1 on success, otherwise 0.
int
heapinsert(Heap *h, void *x)
{
    // 动态扩容
    if (h->len == h->cap) {
        void **ndata;
        size_t ncap = (h->len+1) * 2; /* allocate twice what we need */

        ndata = malloc(sizeof(void*) * ncap);
        if (!ndata) {
            return 0;
        }

        memcpy(ndata, h->data, sizeof(void*) * h->len);
        free(h->data);
        h->data = ndata;
        h->cap = ncap;
    }

    size_t k = h->len; // 下一个空位
    h->len++;
    set(h, k, x);
    siftdown(h, k); // 冒泡
    return 1;
}


void *
heapremove(Heap *h, size_t k)
{
    if (k >= h->len) {
        return 0;
    }

    void *x = h->data[k];
    h->len--; // 下次 insert 会覆盖 last
    set(h, k, h->data[h->len]); // 用 last 替换 K
    siftdown(h, k); // 冒泡原 last
    siftup(h, k); // 下沉原 last，或下沉换下的 parent
    return x;
}
