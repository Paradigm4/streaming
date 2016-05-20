/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
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

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <query/TypeSystem.h>
#include <query/Operator.h>
#include <log4cxx/logger.h>

#include "../lib/slave.h"
#include "../lib/serial.h"

using std::shared_ptr;
using std::make_shared;
using std::string;
using std::ostringstream;
using std::vector;

namespace scidb
{

namespace stream
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.stream"));

/**
 * A class that writes an output array given a sequence of strings
 */
class OutputWriter : public boost::noncopyable
{
private:
    shared_ptr<Array>           _output;
    InstanceID const            _myInstanceId;
    shared_ptr<Query>           _query;
    size_t                      _chunkNo;
    shared_ptr<ArrayIterator>   _arrayIter;

public:
    /**
     * Create from a schema and query context.
     * @param schema must match (2 dims, 1 string att)
     * @param query the query context
     */
    OutputWriter(ArrayDesc const& schema, shared_ptr<Query>& query):
        _output(new MemArray(schema, query)),
        _myInstanceId(query->getInstanceID()),
        _query(query),
        _chunkNo(0),
        _arrayIter(_output->getIterator(0))
    {}

    /**
     * Write the given string into a new chunk of the array
     */
    void writeString(string const& str)
    {
        if(_output.get() == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "stream::OuputWriter::writeString called on an invalid object";
        }
        Coordinates pos(2);
        pos[0] = _myInstanceId;
        pos[1] = _chunkNo;
        shared_ptr<ChunkIterator> chunkIter = _arrayIter->newChunk(pos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
        chunkIter->setPosition(pos);
        Value result;
        result.setString(str);
        chunkIter->writeItem(result);
        chunkIter->flush();
        _chunkNo++;
    }

    /**
     * Retrieve the final array. After this call, this object is invalid.
     * @return the array containing data from all the previous writeString calls
     */
    shared_ptr<Array> finalize()
    {
        _arrayIter.reset();
        shared_ptr<Array> res = _output;
        _output.reset();
        _query.reset();
        return res;
    }
};

/**
 * An abstraction over the slave process forked by SciDB.
 */
class ChildProcess
{
private:
    bool _alive;
    int _pollTimeoutMillis;
    shared_ptr<Query> _query;
    slave _childContext;

public:
    /**
     * Fork a new process.
     * @param commandLine the bash command to execute
     * @param query the query context
     */
    ChildProcess(string const& commandLine, shared_ptr<Query>& query):
        _alive(false),
        _pollTimeoutMillis(100),
        _query(query)
    {
        LOG4CXX_DEBUG(logger, "Executing "<<commandLine);
        _childContext = run (commandLine.c_str(), NULL, NULL);
        if (_childContext.pid < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fork failed, bummer";
        }
        int flags = fcntl(_childContext.out, F_GETFL, 0);
        if(fcntl(_childContext.out, F_SETFL, flags | O_NONBLOCK) < 0 )
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fcntl failed, bummer";
        }
        flags = fcntl(_childContext.in, F_GETFL, 0);
        if(fcntl(_childContext.in, F_SETFL, flags | O_NONBLOCK) < 0 )
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fcntl failed, bummer";
        }
        _alive = true;
    }

    ~ChildProcess()
    {
        terminate();
    }

    /**
     * Tear down the connection and kill the child process. Idempotent.
     */
    void terminate()
    {
        if(_alive)
        {
            _alive = false;
            close (_childContext.in);
            close (_childContext.out);
            kill (_childContext.pid, SIGTERM);
            pid_t res = 0;
            size_t retries = 0;
            while( res == 0 && retries < 50) // allow up to ~0.5 seconds for child to stop
            {
                usleep(10000);
                res = waitpid (_childContext.pid, NULL, WNOHANG);
                ++retries;
            }
            if( res == 0 )
            {
                LOG4CXX_WARN(logger, "child did not exit in time, sending sigkill and waiting indefinitely");
                kill (_childContext.pid, SIGKILL);
                waitpid (_childContext.pid, NULL, 0);
            }
            LOG4CXX_DEBUG(logger, "child killed");
        }
    }

    /**
     * Check the child status.
     * @return true if the child is alive and healthy as far as we know; false otherwise
     */
    bool isAlive()
    {
        return _alive;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    // Low-level binary interface: soft read (up to n bytes), hard read (n bytes), hard write (n bytes)
    ///////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Read up to maxBytes of data from child. The function returns only when there was *some* nonzero
     * amount of data read successfully. The amount of data read may be less than maxBytes if the child
     * is not ready to provide more.
     * @param outputBuf the destination to write data to
     * @param maxBytes the maximum number of bytes to read (must not exceed the size of outputBuf)
     * @return the number of actual bytes read, always > 0
     * @throw if the query was cancelled while reading, or child has exited, or there was a read error
     */
    ssize_t softRead(char* outputBuf, size_t const maxBytes)
    {
        LOG4CXX_TRACE(logger, "Reading from child");
        struct pollfd pollstat [1];
        pollstat[0].fd = _childContext.out;
        pollstat[0].events = POLLIN;
        int ret = 0;
        while( ret == 0 )
        {
            Query::validateQueryPtr(_query); //are we still OK to execute the query?
            //no waitpid here: child is allowed to write the last message, then terminate
            errno = 0;
            ret = poll(pollstat, 1, _pollTimeoutMillis); //chill out until the child gives us some data
        }
        if (ret < 0)
        {
            LOG4CXX_WARN(logger, "STREAM: poll failure errno "<<errno);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "poll failed";
        }
        errno = 0;
        ssize_t nRead = read(_childContext.out, outputBuf, maxBytes);
        if(nRead <= 0)
        {
            LOG4CXX_WARN(logger, "STREAM: child terminated early: read returned "<<nRead <<" errno "<<errno);
            terminate();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "error reading from child";
        }
        LOG4CXX_TRACE(logger, "Read "<<nRead<<" bytes from child");
        return nRead;
    }

    /**
     * Read exactly [bytes] of data from child into outputBuf. Returns only after successful read.
     * @param outputBuf the destination to write data to
     * @param bytes the number of bytes to read (must not exceed the size of outputBuf)
     * @throw if the query was cancelled while reading, or child has exited, or there was a read error
     */
    void hardRead(char* outputBuf, size_t const bytes)
    {
        size_t bytesRead = 0;
        while (bytesRead < bytes)
        {
            bytesRead += softRead(outputBuf + bytesRead, bytes - bytesRead);
        }
    }

    /**
     * Write exactly [bytes] of data from buf to child. Returns only after successful write.
     * @param buf the data to write
     * @param bytes the amount of data to write
     * @throw if the query was cancelled while writing, or child has exited or there was a write error
     */
    void hardWrite(char const* buf, size_t const bytes)
    {
        LOG4CXX_TRACE(logger, "Writing to child");
        size_t bytesWritten = 0;
        while(bytesWritten != bytes)
        {
            struct pollfd pollstat [1];
            pollstat[0].fd = _childContext.in;
            pollstat[0].events = POLLOUT;
            int ret = 0;
            while( ret == 0 )
            {
                Query::validateQueryPtr(_query); //are we still OK to execute the query?
                int status;
                if(waitpid (_childContext.pid, &status, WNOHANG) == _childContext.pid) //that child still there?
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
            size_t writeRet = write(_childContext.in, buf + bytesWritten, bytes - bytesWritten);
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

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // High-level TSV read and write
    // The format is an integer number of lines, folllowed by newline, followed by the data, i.e.:
    // 3
    // a    0
    // b    1
    // c    2
    //
    // The parent sends a message in the above format to the child; the child then responds with
    // a different message. The child must consume the whole message first, then send a response.
    // When there is no more data to send, the parent sends one message with
    // 0
    // That denotes the last exchange. The child may then respond with 0 or a larger message.
    ////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Write a TSV message to the child.
     * @param nLines number of lines to write (0 for last message)
     * @param inputData data to write (if n > 0)
     * @throw if there's a query cancellation or write error
     */
    void writeTSV(size_t const nLines, string const& inputData)
    {
        char hdr[4096];
        snprintf (hdr, 4096, "%lu\n", nLines);
        size_t n = strlen (hdr);
        hardWrite (hdr, n);
        if(n>0)
        {
            hardWrite (inputData.c_str(), inputData.size());
        }
    }

    /**
     * Read a TSV message from the child. Expects the first line to contain exactly the number of following
     * lines (which needs to be written atomically), followed by exactly that many lines of text, terminated with newline.
     * @param[out] output set to the result, may be zero-sized
     * @throw if there's a query cancellation or read error
     */
    void readTSV(string& output)
    {
        size_t bufSize = 8*1024*1024;
        vector<char> buf(bufSize);
        size_t dataSize = softRead( &(buf[0]), bufSize);
        size_t idx =0;
        while ( idx < dataSize && buf[idx] != '\n')
        {
            ++ idx;
        }
        if( idx >= dataSize)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "TSV header provided by child did not contain a newline";
        }
        char* end = &(buf[0]);
        errno = 0;
        int64_t expectedNumLines = strtoll(&(buf[0]), &end, 10);
        if(*end != '\n' || (size_t) (end - &(buf[0])) != idx || errno !=0 || expectedNumLines < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child provided invalid number of lines";
        }
        ++idx;
        size_t const tsvStartIdx = idx;
        int64_t linesReceived = 0;
        while(linesReceived < expectedNumLines)
        {
            while(idx < dataSize && linesReceived < expectedNumLines)
            {
                if(buf[idx] == '\n')
                {
                    ++linesReceived;
                }
                ++idx;
            }
            if(linesReceived < expectedNumLines)
            {
                if(idx >= bufSize)
                {
                    bufSize = bufSize * 2;
                    buf.resize(bufSize);
                }
                dataSize += softRead( &(buf[0]) + dataSize, bufSize - dataSize);
            }
        }
        if(dataSize > idx)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received extraneous characters at end of message";
        }
        if(buf[dataSize-1] != '\n')
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child did not end message with newline";
        }
        output.assign( &(buf[tsvStartIdx]), dataSize - tsvStartIdx);
    }
};

class TextChunkConverter
{
private:
    enum AttType
    {
        OTHER   =0,
        STRING =1,
        FLOAT  =2,
        DOUBLE =3,
        BOOL   =4,
        UINT8  =5,
        INT8    =6
    };

    char const              _attDelim;
    char const              _lineDelim;
    bool const              _printCoords;
    bool const              _quoteStrings;
    size_t const            _precision;
    size_t const            _nAtts;
    vector<AttType>         _attTypes;
    vector<FunctionPointer> _converters;
    Value                   _stringBuf;
    string                  _nanRepresentation;
    string                  _nullRepresentation;

public:
    TextChunkConverter(ArrayDesc const& inputArrayDesc):
       _attDelim(  '\t'),
       _lineDelim( '\n'),
       _printCoords(false),
       _quoteStrings(true),
       _precision(10),
       _nAtts(inputArrayDesc.getAttributes(true).size()),
       _attTypes(_nAtts, OTHER),
       _converters(_nAtts, 0),
       _nanRepresentation("nan"),
       _nullRepresentation("null")
    {
        Attributes const& inputAttrs = inputArrayDesc.getAttributes(true);
        for (size_t i = 0; i < inputAttrs.size(); ++i)
        {
            if(inputAttrs[i].getType() == TID_STRING)
            {
                _attTypes[i] = STRING;
            }
            else if(inputAttrs[i].getType() == TID_BOOL)
            {
                _attTypes[i] = BOOL;
            }
            else if(inputAttrs[i].getType() == TID_DOUBLE)
            {
                _attTypes[i] = DOUBLE;
            }
            else if(inputAttrs[i].getType() == TID_FLOAT)
            {
                _attTypes[i] = FLOAT;
            }
            else if(inputAttrs[i].getType() == TID_UINT8)
            {
                _attTypes[i] = UINT8;
            }
            else if(inputAttrs[i].getType() == TID_INT8)
            {
                _attTypes[i] = INT8;
            }
            else
            {
                _converters[i] = FunctionLibrary::getInstance()->findConverter(
                    inputAttrs[i].getType(),
                    TID_STRING,
                    false);
            }
        }
    }

    ~TextChunkConverter()
    {}

    void convertChunk(vector<shared_ptr<ConstChunkIterator> > citers, size_t &nCells, string& output)
    {
        nCells = 0;
        ostringstream outputBuf;
        outputBuf.precision(_precision);
        while(!citers[0]->end())
        {
            if(_printCoords)
            {
                Coordinates const& pos = citers[0]->getPosition();
                for(size_t i =0, n=pos.size(); i<n; ++i)
                {
                    if(i)
                    {
                        outputBuf<<_attDelim;
                    }
                    outputBuf<<pos[i];
                }
            }
            for (size_t i = 0; i < _nAtts; ++i)
            {
                Value const& v = citers[i]->getItem();
                if (i || _printCoords)
                {
                    outputBuf<<_attDelim;
                }
                if(v.isNull())
                {
                    outputBuf<<_nullRepresentation; //TODO: all missing codes are converted to this representation
                }
                else
                {
                    switch(_attTypes[i])
                    {
                    case STRING:
                        if(_quoteStrings)
                        {
                            char const* s = v.getString();
                            outputBuf << '\'';
                            while (char c = *s++)
                            {
                                if (c == '\'')
                                {
                                    outputBuf << '\\' << c;
                                }
                                else if (c == '\\')
                                {
                                    outputBuf << "\\\\";
                                }
                                else
                                {
                                    outputBuf << c;
                                }
                            }
                            outputBuf << '\'';
                        }
                        else
                        {
                            outputBuf<<v.getString();
                        }
                        break;
                    case BOOL:
                        if(v.getBool())
                        {
                            outputBuf<<"true";
                        }
                        else
                        {
                            outputBuf<<"false";
                        }
                        break;
                    case DOUBLE:
                        {
                            double nbr =v.getDouble();
                            if(std::isnan(nbr))
                            {
                                outputBuf<<_nanRepresentation;
                            }
                            else
                            {
                                outputBuf<<nbr;
                            }
                        }
                        break;
                    case FLOAT:
                        {
                            float fnbr =v.getFloat();
                            if(std::isnan(fnbr))
                            {
                                outputBuf<<_nanRepresentation;
                            }
                            else
                            {
                                outputBuf<<fnbr;
                            }
                        }
                        break;
                    case UINT8:
                        {
                            uint8_t nbr =v.getUint8();
                            outputBuf<<(int16_t) nbr;
                        }
                        break;
                    case INT8:
                        {
                            int8_t nbr =v.getUint8();
                            outputBuf<<(int16_t) nbr;
                        }
                        break;
                    case OTHER:
                        {
                            Value const * vv = &v;
                            (*_converters[i])(&vv, &_stringBuf, NULL);
                            outputBuf<<_stringBuf.getString();
                        }
                    }
                }
            }
            outputBuf<<_lineDelim;
            ++nCells;
            for(size_t i = 0; i<_nAtts; ++i)
            {
                ++(*citers[i]);
            }
        }
        output = outputBuf.str();
    }
};

}

using namespace stream;

class PhysicalStream : public PhysicalOperator
{
public:
    PhysicalStream(std::string const& logicalName,
        std::string const& physicalName,
        Parameters const& parameters,
        ArrayDesc const& schema):
            PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
{
    shared_ptr<Array>& inputArray = inputArrays[0];
    string command = ((shared_ptr<OperatorParamPhysicalExpression>&) _parameters[0])->getExpression()->evaluate().getString();
    ArrayDesc const& inputSchema = inputArray ->getArrayDesc();
    size_t const nAttrs = inputSchema.getAttributes(true).size();
    vector <shared_ptr<ConstArrayIterator> > aiters (nAttrs);
    for(size_t i =0; i<nAttrs; ++i)
    {
        aiters[i] = inputArray->getConstIterator(i);
    }
    vector <shared_ptr<ConstChunkIterator> > citers (nAttrs);
    TextChunkConverter converter(inputSchema);
    OutputWriter outputWriter(_schema, query);
    ChildProcess child(command, query);
    bool slaveAlive = child.isAlive();
    string tsvInput;
    string output;
    size_t nCells=0;
    while(!aiters[0]->end() && slaveAlive)
    {
        for(size_t i =0; i<nAttrs; ++i)
        {
            citers[i] = aiters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
        }
        converter.convertChunk(citers, nCells, tsvInput);
        child.writeTSV(nCells, tsvInput);
        child.readTSV(output);
        if(output.size())
        {
            if(output.size() > 1024*1024*1024)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "cannot receive output exceeding 1GB";
            }
            output.resize(output.size()-1);
            outputWriter.writeString(output);
        }
        for(size_t i =0; i<nAttrs; ++i)
        {
           ++(*aiters[i]);
        }
    }
    child.writeTSV(0, "");
    child.readTSV(output);
    if(output.size())
    {
        if(output.size() > 1024*1024*1024)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "cannot receive output exceeding 1GB";
        }
        output.resize(output.size()-1);
        outputWriter.writeString(output);
    }
    child.terminate();
    return outputWriter.finalize();
}
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalStream, "stream", "PhysicalStream");

} // end namespace scidb
