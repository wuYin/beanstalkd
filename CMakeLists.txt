cmake_minimum_required(VERSION 3.12)
project(beanstalkd C)
set(versc "vers.c")
string(TOLOWER ${CMAKE_SYSTEM_NAME} os)
if (NOT "${PROJECT_SOURCE_DIR}/${versc}")
    execute_process(COMMAND ./verc.sh > ${versc})
endif ()
add_executable(beanstalkd
        ${os}.c
        ${versc}
        conn.c
        file.c
        heap.c
        job.c
        main.c
        ms.c
        net.c
        primes.c
        prot.c
        serv.c
        time.c
        tube.c
        util.c
        walg.c
#        linux.c
        dat.h
        )
