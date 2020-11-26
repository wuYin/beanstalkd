#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>

struct Server srv = {
    .port = Portdef,
    .wal = {
        .filesize = Filesizedef,
        .wantsync = 1,
        .syncrate = DEFAULT_FSYNC_MS * 1000000,
    },
};

// srv_acquire_wal tries to lock the wal dir specified by s->wal and
// replay entries from it to initialize the s state with jobs.
// On errors it exits from the program.
void srv_acquire_wal(Server *s) {
    if (s->wal.use) {
        // We want to make sure that only one beanstalkd tries
        // to use the wal directory at a time. So acquire a lock
        // now and never release it.
        if (!waldirlock(&s->wal)) {
            twarnx("failed to lock wal dir %s", s->wal.dir);
            exit(10);
        }

        Job list = {.prev=NULL, .next=NULL};
        list.prev = list.next = &list;
        walinit(&s->wal, &list);
        int ok = prot_replay(s, &list);
        if (!ok) {
            twarnx("failed to replay log");
            exit(1);
        }
    }
}

void
srvserve(Server *s)
{
    // 1. create epoll fd
    if (sockinit() == -1) {
        twarnx("sockinit");
        exit(1);
    }

    // 2. socket s prepare to accept conn
    s->sock.x = s;
    s->sock.f = (Handle)srvaccept;
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;

    if (sockwant(&s->sock, 'r') == -1) {
        twarn("sockwant");
        exit(2);
    }

    // 4. block event loop
    Socket *sock; // sock -> *Socket -> Socket
    for (;;) {
        // 4.1 calculate next epoll timeout
        int64 period = prottick(s);

        // 4.2 try to wait events happened in sock
        int rw = socknext(&sock, period);
        if (rw == -1) {
            twarnx("socknext");
            exit(1);
        }

        // 4.3 handle event
        if (rw) {
            sock->f(sock->x, rw);
        }
    }
}


// NOTE: handle client connection
void
srvaccept(Server *s, int ev)
{
    h_accept(s->sock.fd, ev, s);
}
