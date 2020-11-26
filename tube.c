#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// NOTE: tubes maintains all tubes created by cur beanstalkd
struct Ms tubes;

// NOTE: truncate tube name
// NOTE: set ready/delay heap less comparators
// NOTE: init buried linked list and waiting_conns ms
Tube *
make_tube(const char *name)
{
    Tube *t = new(Tube);
    if (!t)
        return NULL;

    strncpy(t->name, name, MAX_TUBE_NAME_LEN);
    if (t->name[MAX_TUBE_NAME_LEN - 1] != '\0') {
        t->name[MAX_TUBE_NAME_LEN - 1] = '\0';
        twarnx("truncating tube name");
    }

    t->ready.less = job_pri_less;
    t->delay.less = job_delay_less;
    t->ready.setpos = job_setpos;
    t->delay.setpos = job_setpos;

    Job j = {.tube = NULL};
    t->buried = j;
    t->buried.prev = t->buried.next = &t->buried;
    ms_init(&t->waiting_conns, NULL, NULL);

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

// NOTE: decr ref count of t, free it if ref is 0, like last client also ignore tube t
void
tube_dref(Tube *t)
{
    if (!t) return;
    if (t->refs < 1) {
        twarnx("refs is zero for tube: %s", t->name);
        return;
    }

    --t->refs;
    if (t->refs < 1)
        tube_free(t); // NOTE: gc of tubes
}

void
tube_iref(Tube *t)
{
    if (!t) return;
    ++t->refs;
}

// NOTE: create tube and weak append into global tube list
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
    r = ms_append(&tubes, t);
    if (!r)
        // NOTE: oom and register failed, delete the new tube
        return tube_dref(t), (Tube *) 0;

    return t;
}

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

