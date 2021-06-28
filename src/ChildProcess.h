/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2021 Paradigm4 Inc.
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

#ifndef CHILDPROCESS_H_
#define CHILDPROCESS_H_

#include <query/PhysicalOperator.h>
#include <unistd.h>

namespace scidb { namespace stream
{

/**
 * An abstraction over the child process forked by SciDB.
 */
class ChildProcess
{
public:
    /**
     * Fork a new process.
     * @param commandLine the bash command to execute
     * @param query the query context
     * @param readBufSize the size of the buffer used for reading
     */
    ChildProcess(std::string const& commandLine, std::shared_ptr<Query>& query, size_t const readBufSize = 1024*1024);
    ~ChildProcess()
    {
        terminate();
    }

    /**
     * Tear down the connection and kill the child process. Idempotent.
     */
    void terminate();

    /**
     * Check the child status.
     * @return true if the child is alive and healthy as far as we know; false otherwise
     */
    bool isAlive()
    {
        return _alive;
    }

    /**
     * Read up to maxBytes of data from child. The function returns only when there was *some* nonzero
     * amount of data read successfully. The amount of data read may be less than maxBytes if the child
     * is not ready to provide more. The reads are buffered so this may be alled frequently.
     * @param outputBuf the destination to write data to
     * @param maxBytes the maximum number of bytes to read (must not exceed the size of outputBuf)
     * @param throwIfChildDead check that the child process is running and throw if it is not running.
     *                         Switched to false when reading the last message from the child.
     * @return the number of actual bytes read, always > 0
     * @throw if the query was cancelled while reading, or child has exited, or there was a read error
     */
    size_t softRead(void* outputBuf, size_t const maxBytes, bool throwIfChildDead = true)
    {
        if(_readBufIdx == _readBufEnd)
        {
            readIntoBuf(throwIfChildDead);
        }
        size_t bytesToReturn = _readBufEnd - _readBufIdx;
        bytesToReturn = maxBytes < bytesToReturn ? maxBytes : bytesToReturn;
        memcpy(outputBuf, &_readBuf[_readBufIdx], bytesToReturn);
        _readBufIdx += bytesToReturn;
        return bytesToReturn;
    }

    /**
     * Read exactly [bytes] of data from child into outputBuf. Returns only after successful read.
     * The reads are buffered so this may be alled frequently.
     * @param outputBuf the destination to write data to
     * @param bytes the number of bytes to read (must not exceed the size of outputBuf)
     * @param throwIfChildDead check that the child process is running and throw if it is not running.
     *                         Switched to false when reading the last message from the child.
     * @throw if the query was cancelled while reading, or child has exited, or there was a read error
     */
    void hardRead(void* outputBuf, size_t const bytes, bool throwIfChildDead = true)
    {
        size_t bytesRead = 0;
        while (bytesRead < bytes)
        {
            bytesRead += softRead(((char*) outputBuf) + bytesRead, bytes - bytesRead, throwIfChildDead);
        }
    }

    /**
     * Write exactly [bytes] of data from buf to child. Returns only after successful write. The writes
     * are *NOT* buffered - so the caller should coalesce data into large chunks before writing.
     * @param inputBuf the data to write
     * @param bytes the amount of data to write
     * @throw if the query was cancelled while writing, or child has exited or there was a write error
     */
    void hardWrite(void const* inputBuf, size_t const bytes);

private:
    bool  _alive;
    int const _pollTimeoutMillis;
    std::shared_ptr<Query> _query;
    std::vector <char> _readBuf;
    size_t _readBufIdx;
    size_t _readBufEnd;
    pid_t _childPid;
    int   _childInFd;
    int   _childOutFd;

    void readIntoBuf(bool throwIfChildDead);
};

} } //namespace

#endif /* CHILDPROCESS_H_ */
