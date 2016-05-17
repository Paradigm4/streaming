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
class SlaveProcess
{
private:
    bool _alive;
    int _pollTimeoutMillis;
    shared_ptr<Query> _query;
    slave _childContext;

public:
    /**
     * Fork a new process.
     * @param commandLine a single executable file at the moment. Just put it all in a script, bro.
     */
    SlaveProcess(string const& commandLine, shared_ptr<Query>& query):
        _alive(false),
        _pollTimeoutMillis(100),
        _query(query)
    {
        string commandLineCopy = commandLine;
        char* argv[2];
        argv[0] = const_cast<char*>(commandLine.c_str());
        argv[1] = NULL;
        _childContext = run (argv, NULL, NULL);
        if (_childContext.pid < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "fork failed, bummer";
        }
        _alive = true;
        int flags = fcntl(_childContext.out, F_GETFL, 0);
        fcntl(_childContext.out, F_SETFL, flags | O_NONBLOCK);
    }

    ~SlaveProcess()
    {
        terminate();
    }

    /**
     * @return true if the child is alive and healthy as far as we know; false otherwise
     */
    bool isAlive()
    {
        return _alive;
    }

    /**
     * Read from child while checking the query context for cancellation. If the query is cancelled
     * while waiting for data, the child is terminated and an exception is thrown. If the child is
     * finished, or there is read error, 0 or -1 is returned respectively, and the child is terminated.
     * @param outputBuf the destination to write data to
     * @param maxBytes the maximum number of bytes to read (must not exceed the size of outputBuf
     * @return the number of actual bytes read, -1 if an error occured, 0 if done or EOF.
     */
    ssize_t readBytesFromChild(char* outputBuf, size_t const maxBytes)
    {
        struct pollfd pollstat [1];
        pollstat[0].fd = _childContext.out;
        pollstat[0].events = POLLIN;
        int ret = 0;
        while( ret == 0 )
        {
            try
            {
                Query::validateQueryPtr(_query); //are we still OK to execute the query?
            }
            catch(...)
            {
                terminate(); //oh no, we're not OK! Shoot the child on our way down.
                throw;
            }
            errno = 0;
            ret = poll(pollstat, 1, _pollTimeoutMillis); //chill out until the child gives us some data
        }
        if (ret < 0)
        {
            LOG4CXX_DEBUG(logger, "STREAM: poll failure errno "<<errno);
            terminate();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "poll failed";
        }
        errno = 0;
        ssize_t nRead = read(_childContext.out, outputBuf, maxBytes);
        if(nRead <= 0)
        {
            LOG4CXX_DEBUG(logger, "STREAM: child terminated early: read "<<nRead <<" errno "<<errno);
            terminate();
        }
        return nRead;
    }

    /**
     * Read a full-formatted TSV message from child. Expects the first line to contain the number of following
     * lines (which needs to be written atomically and less than 4096 characters), followed by exactly that many
     * lines of text, terminated with newline.
     * @param[out] output set to the result
     * @return true if the read was successful and output is set to the valid TSV string (minus the header number),
     *         false if the read failed in which case output is not modified.
     */
    bool readTsvFromChild(string& output)
    {
        size_t bufSize = 4096;
        vector<char> buf(bufSize);
        ssize_t numRead = readBytesFromChild( &(buf[0]), bufSize);
        if (numRead <= 0)
        {
            LOG4CXX_DEBUG(logger, "STREAM: child failed to atomically write a proper TSV header; retcode '"<<numRead<<"'");
            terminate();
            return false;
        }
        size_t occupied = numRead, idx =0;
        while ( idx < occupied && buf[idx] != '\n')
        {
            ++ idx;
        }
        if( idx >= occupied || buf[idx] != '\n')
        {
            LOG4CXX_DEBUG(logger, "STREAM: child failed to atomically write a proper TSV header");
            terminate();
            return false;
        }
        char* end = &(buf[0]);
        errno = 0;
        int64_t expectedNumLines = strtoll(&(buf[0]), &end, 10);
        if(*end != '\n' || (size_t) (end - &(buf[0])) != idx || errno !=0 || expectedNumLines < 0)
        {
            LOG4CXX_DEBUG(logger, "STREAM: child failed to provide number of lines");
            terminate();
            return false;
        }
        ++idx;
        size_t const tsvStartIdx = idx;
        int64_t linesReceived = 0;
        while(linesReceived < expectedNumLines)
        {
            while(idx < occupied && linesReceived < expectedNumLines)
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
                numRead = readBytesFromChild( &(buf[0]) + occupied, bufSize - occupied);
                if (numRead <= 0)
                {
                    LOG4CXX_DEBUG(logger, "STREAM: failed to fetch subsequent lines");
                    terminate();
                    return false;
                }
                occupied += numRead;
            }
        }
        if(buf[occupied-1] != '\n')
        {
            LOG4CXX_DEBUG(logger, "STREAM: child failed to end with newline");
            terminate();
            return false;
        }
        output.assign( &(buf[tsvStartIdx]), occupied - tsvStartIdx);
        return true;
    }

    bool tsvExchange(size_t const nLines, char const* inputData, string& outputData)
    {
        ssize_t ret = write_tsv(_childContext.in, inputData, nLines);
        if(ret<0)
        {
            LOG4CXX_DEBUG(logger, "STREAM: child terminated early: write_tsv");
            terminate();
            return false;
        }
        return readTsvFromChild(outputData);
    }

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
    SlaveProcess slave(command, query);
    bool slaveAlive = slave.isAlive();
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
        slaveAlive = slave.tsvExchange(nCells, tsvInput.c_str(), output);
        if(slaveAlive)
        {
            outputWriter.writeString(output);
        }
        for(size_t i =0; i<nAttrs; ++i)
        {
           ++(*aiters[i]);
        }
    }
    slave.terminate();
    return outputWriter.finalize();
}
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalStream, "stream", "PhysicalStream");

} // end namespace scidb
