#define _XOPEN_SOURCE 600

#include "dat.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>

#ifndef EPOLLRDHUP
#define EPOLLRDHUP 0x2000 // NOTE: client proactive closed conn from linux2.6.7
#endif

static int epfd;


int
sockinit(void)
{
    epfd = epoll_create(1);
    if (epfd == -1) {
        twarn("epoll_create");
        return -1;
    }
    return 0;
}


// 新增、删除或修改 socket 上注册的 epoll event
int
sockwant(Socket *s, int rw)
{
    int op;

    if (!s->added && !rw) {
        return 0;
    } else if (!s->added && rw) {
        s->added = 1;
        op = EPOLL_CTL_ADD;
    } else if (!rw) {
        op = EPOLL_CTL_DEL;
    } else {
        op = EPOLL_CTL_MOD;
    }

    struct epoll_event ev = {.events=0};
    switch (rw) {
    case 'r':
        ev.events = EPOLLIN;
        break;
    case 'w':
        ev.events = EPOLLOUT;
        break;
    }
    ev.events |= EPOLLRDHUP | EPOLLPRI;
    ev.data.ptr = s; // Socket attach 到 event

    return epoll_ctl(epfd, op, s->fd, &ev);
}


int
socknext(Socket **s, int64 timeout)
{
    int r;
    struct epoll_event ev = {.events=0};

    r = epoll_wait(epfd, &ev, 1, (int)(timeout/1000000));
    if (r == -1 && errno != EINTR) {
        twarn("epoll_wait");
        exit(1);
    }

    if (r > 0) {
        // 分离 event 中的 Socket
        *s = ev.data.ptr;
        if (ev.events & (EPOLLHUP|EPOLLRDHUP)) { // connection closed
            return 'h';
        } else if (ev.events & EPOLLIN) {
            return 'r';
        } else if (ev.events & EPOLLOUT) {
            return 'w';
        }
    }
    return 0;
}
