#include "dat.h"
#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>
#include <fcntl.h>

static void
su(const char *user) 
{
    errno = 0;
    struct passwd *pwent = getpwnam(user);
    if (errno) {
        twarn("getpwnam(\"%s\")", user);
        exit(32);
    }
    if (!pwent) {
        twarnx("getpwnam(\"%s\"): no such user", user);
        exit(33);
    }

    int r = setgid(pwent->pw_gid);
    if (r == -1) {
        twarn("setgid(%d \"%s\")", pwent->pw_gid, user);
        exit(34);
    }

    r = setuid(pwent->pw_uid);
    if (r == -1) {
        twarn("setuid(%d \"%s\")", pwent->pw_uid, user);
        exit(34);
    }
}

static void
handle_sigterm_pid1()
{
    exit(143);
}

static void
set_sig_handlers()
{
    struct sigaction sa;

    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    int r = sigemptyset(&sa.sa_mask);
    if (r == -1) {
        twarn("sigemptyset()");
        exit(111);
    }

    r = sigaction(SIGPIPE, &sa, 0);
    if (r == -1) {
        twarn("sigaction(SIGPIPE)");
        exit(111);
    }

    // 收到 SIGUSR1 信号后进入 drain 模式
    sa.sa_handler = enter_drain_mode;
    r = sigaction(SIGUSR1, &sa, 0);
    if (r == -1) {
        twarn("sigaction(SIGUSR1)");
        exit(111);
    }

    // Workaround for running the server with pid=1 in Docker.
    // Handle SIGTERM so the server is killed immediately and
    // not after 10 seconds timeout. See issue #527.
    if (getpid() == 1) {
        // 在 docker 运行时收到 SIGTERM 直接 exit 进程
        sa.sa_handler = handle_sigterm_pid1;
        r = sigaction(SIGTERM, &sa, 0);
        if (r == -1) {
            twarn("sigaction(SIGTERM)");
            exit(111);
        }
    }
}

int
main(int argc, char **argv)
{
    UNUSED_PARAMETER(argc);

    progname = argv[0];
    setlinebuf(stdout);

    // 1. 解析命令行参数
    optparse(&srv, argv+1);
    if (verbose) {
        printf("pid %d\n", getpid());
    }

    // 2. 创建 server socket 并绑定监听
    int r = make_server_socket(srv.addr, srv.port);
    if (r == -1) {
        twarnx("make_server_socket()");
        exit(111);
    }

    srv.sock.fd = r;

    // 3. 初始化实例，创建 tubes, default tube
    prot_init();

    if (srv.user)
        su(srv.user);
    set_sig_handlers();

    // 4. 从 WAL 恢复
    srv_acquire_wal(&srv);

    // 5. 启动 server 执行 eventloop
    srvserve(&srv);
    exit(0);
}
