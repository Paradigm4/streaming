/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2020 Paradigm4 Inc.
* All Rights Reserved.
*
* stream is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* stream is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* stream is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with stream.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include "ChildProcess.h"
#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <query/Query.h>

using std::shared_ptr;
using std::string;

namespace scidb { namespace stream
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.stream.childprocess"));

ChildProcess::ChildProcess(string const& commandLine, shared_ptr<Query>& query, size_t const readBufSize):
        _alive(false),
        _pollTimeoutMillis(100),
        _query(query),
        _readBuf(readBufSize),
        _readBufIdx(0),
        _readBufEnd(0)
{
    LOG4CXX_DEBUG(logger, "Executing "<<commandLine);
    int parent_child[2];          // pipe descriptors parent writes to child
    int child_parent[2];          // pipe descriptors child writes to parent
    pipe (parent_child);
    pipe (child_parent);
    _childPid = ::fork ();
    switch (_childPid )
    {
    case -1:
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fork failed, bummer";
    case 0:                    // child
        close (1);
        dup (child_parent[1]);    // stdout writes to parent
        close (0);
        dup (parent_child[0]);    // parent writes to stdin
        close (parent_child[1]);
        close (child_parent[0]);
        //child needs to close all open FDs - just in case its parent is listening on a port (ahem)
        //we wouldn't want the child to clog up said port for no reason
        struct rlimit limit;
        getrlimit(RLIMIT_NOFILE, &limit);
        for(unsigned long i = 3; i<limit.rlim_max; i = i+1)
        {
            close(i);
        }
        execle ("/bin/bash", "/bin/bash", "-c", commandLine.c_str(), NULL, NULL);
        abort ();  //if execle returns, it means we're in trouble. bail asap.
        break;
    default:  // parent
        close (parent_child[0]);
        close (child_parent[1]);
        _childInFd  = parent_child[1];
        _childOutFd = child_parent[0];
        int flags = fcntl(_childOutFd, F_GETFL, 0);
        if(fcntl(_childOutFd, F_SETFL, flags | O_NONBLOCK) < 0 )
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fcntl failed, bummer";
        }
        flags = fcntl(_childInFd, F_GETFL, 0);
        if(fcntl(_childInFd, F_SETFL, flags | O_NONBLOCK) < 0 )
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fcntl failed, bummer";
        }
    }
    _alive = true;
}

void ChildProcess::terminate()
{
    if(_alive)
    {
        _alive = false;
        close (_childInFd);
        close (_childOutFd);
        kill (_childPid, SIGTERM);
        pid_t res = 0;
        size_t retries = 0;
        while( res == 0 && retries < 50) // allow up to ~0.5 seconds for child to stop
        {
            usleep(10000);
            res = waitpid (_childPid, NULL, WNOHANG);
            ++retries;
        }
        if( res == 0 )
        {
            LOG4CXX_WARN(logger, "child did not exit in time, sending sigkill and waiting indefinitely");
            kill (_childPid, SIGKILL);
            waitpid (_childPid, NULL, 0);
        }
        LOG4CXX_DEBUG(logger, "child terminated");
    }
}

void ChildProcess::readIntoBuf(bool throwIfChildDead)
{
    _readBufIdx = 0;
    _readBufEnd = 0;
    LOG4CXX_TRACE(logger, "read into buf from child");
    if(!isAlive())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: attempt to read froom dead child";
    }
    struct pollfd pollstat [1];
    pollstat[0].fd = _childOutFd;
    pollstat[0].events = POLLIN;
    int ret = 0;
    while( ret == 0 )
    {
        Query::validateQueryPtr(_query); //are we still OK to execute the query?
        int status;
        if(throwIfChildDead && waitpid (_childPid, &status, WNOHANG) == _childPid) //that child still there?
        {
            terminate();
            LOG4CXX_WARN(logger, "Child terminated while reading; status "<<status);
            if(WIFEXITED(status))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child process terminated early (regular exit)";
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child process terminated early (error)";
        }
        errno = 0;
        ret = poll(pollstat, 1, _pollTimeoutMillis); //chill out until the child gives us some data
    }
    if (ret < 0)
    {
        LOG4CXX_WARN(logger, "STREAM: poll failure errno "<<errno);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "poll failed";
    }
    errno = 0;
    ssize_t nRead = read(_childOutFd, &_readBuf[0], _readBuf.size());
    if(nRead <= 0)
    {
        LOG4CXX_WARN(logger, "STREAM: child terminated early: read returned "<<nRead <<" errno "<<errno);
        terminate();
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "error reading from child";
    }
    LOG4CXX_TRACE(logger, "Read "<<nRead<<" bytes from child");
    _readBufEnd = nRead;
}

void ChildProcess::hardWrite(void const* buf, size_t const bytes)
{
    if(!isAlive())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: attempt to write to dead child";
    }
    LOG4CXX_TRACE(logger, "Writing to child");
    size_t bytesWritten = 0;
    while(bytesWritten != bytes)
    {
        struct pollfd pollstat [1];
        pollstat[0].fd = _childInFd;
        pollstat[0].events = POLLOUT;
        int ret = 0;
        while( ret == 0 )
        {
            Query::validateQueryPtr(_query); //are we still OK to execute the query?
            int status;
            if(waitpid (_childPid, &status, WNOHANG) == _childPid) //that child still there?
            {
                terminate();
                LOG4CXX_WARN(logger, "Child terminated while writing; status "<<status);
                if(WIFEXITED(status))
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child process terminated early (regular exit)";
                }
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child process terminated early (error)";
            }
            errno = 0;
            ret = poll(pollstat, 1, _pollTimeoutMillis); //chill out until the child can accept some data
        }
        if (ret < 0)
        {
            LOG4CXX_WARN(logger, "STREAM: poll failure errno "<<errno);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "poll failed";
        }
        errno = 0;
        size_t writeRet = write(_childInFd, ((char const *)buf) + bytesWritten, bytes - bytesWritten);
        if(writeRet <= 0)
        {
            LOG4CXX_WARN(logger, "STREAM: child terminated early: write returned "<<writeRet <<" errno "<<errno);
            terminate();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "error writing to child";
        }
        bytesWritten += writeRet;
        LOG4CXX_TRACE(logger, "Write iteration");
    }
    LOG4CXX_TRACE(logger, "Wrote "<<bytes<<" bytes to child");
}

}} //namespaces
