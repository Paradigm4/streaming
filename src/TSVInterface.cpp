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

#include "StreamSettings.h"
#include "ChildProcess.h"
#include <vector>
#include <string>
#include "TSVInterface.h"
#include <array/MemArray.h>
#include <query/Query.h>

using std::vector;
using std::shared_ptr;
using std::string;

namespace scidb { namespace stream {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.stream.tsv_interface"));

ArrayDesc TSVInterface::getOutputSchema(vector<ArrayDesc> const& inputSchemas, Settings const& settings, shared_ptr<Query> const& query)
{
    if(settings.getFormat() != TSV)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "TSV interface invoked on improper format";
    }
    if(settings.getTypes().size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "TSV interface does not support the types parameter";
    }
    if(settings.isChunkSizeSet())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "TSV interface does not support the chunk size parameter";
    }
    if(settings.getNames().size() > 1)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "TSV interface supports only one result name";
    }
    Dimensions outputDimensions;
    outputDimensions.push_back(DimensionDesc("instance_id", 0,   query->getInstancesCount()-1, 1, 0));
    outputDimensions.push_back(DimensionDesc("chunk_no",    0,   CoordinateBounds::getMax(),   1, 0));
    Attributes outputAttributes;
    outputAttributes.push_back( AttributeDesc(settings.getNames().size() ? settings.getNames()[0] : "response",   TID_STRING,    0, CompressorType::NONE));
    outputAttributes.addEmptyTagAttribute();
    return ArrayDesc(inputSchemas[0].getName(), outputAttributes, outputDimensions, createDistribution(defaultDistType()), query->getDefaultArrayResidency());
}

TSVInterface::TSVInterface(Settings const& settings, ArrayDesc const& outputSchema, std::shared_ptr<Query> const& query):
    _attDelim(  '\t'),
    _lineDelim( '\n'),
    _printCoords(false),
    _nanRepresentation("nan"),
    _nullRepresentation("\\N"),
    _query(query),
    _result(new MemArray(outputSchema, query)),
    _aiter(_result->getIterator(outputSchema.getAttributes(true).firstDataAttribute())),
    _outPos{ ((Coordinate) _query->getInstanceID()), 0}
{}

void TSVInterface::setInputSchema(ArrayDesc const& inputSchema)
{
    Attributes const& attrs = inputSchema.getAttributes(true);
    _inputTypes.resize(attrs.size());
    _inputConverters.resize(attrs.size());
//    for(size_t i=0; i<_inputTypes.size(); ++i)
    size_t i = 0;
    for (const auto& attr : attrs)
    {
        TypeId const& inputType = attr.getType();
        _inputTypes[i] = typeId2TypeEnum(inputType, true);
        switch(_inputTypes[i])
        {
        case TE_BOOL:
        case TE_DOUBLE:
        case TE_FLOAT:
        case TE_UINT8:
        case TE_INT8:
            _inputConverters[i] = NULL;
            break;
        default:
            _inputConverters[i] = FunctionLibrary::getInstance()->findConverter(
                inputType,
                TID_STRING,
                false);
        }
        i++;
    }
}

void TSVInterface::streamData(std::vector<ConstChunk const*> const& inputChunks, ChildProcess& child)
{
    if(inputChunks.size() != _inputTypes.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received inconsistent number of input chunks";
    }
    if(inputChunks[0]->count() == 0)
    {
        return;
    }
    if(!child.isAlive())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child exited early";
    }
    vector<shared_ptr<ConstChunkIterator> > citers(inputChunks.size());
    size_t nCells;
    string output;
    for(size_t i =0, n= _inputTypes.size(); i<n; ++i)
    {
        citers[i] = inputChunks[i]->getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
    }
    convertChunks(citers, nCells, output);
    writeTSV(nCells, output, child);
    readTSV(output, child);
    if(output.size() > MAX_RESPONSE_SIZE)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "response from child exceeds maximum size";
    }
    if(output.size())
    {
        output.resize(output.size()-1);
        addChunkToArray(output);
    }
}

shared_ptr<Array> TSVInterface::finalize(ChildProcess& child)
{
    writeTSV(0, "", child);
    string output;
    readTSV(output, child, true);
    if(output.size() > MAX_RESPONSE_SIZE)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "response from child exceeds maximum size";
    }
    if(output.size())
    {
        output.resize(output.size()-1);
        addChunkToArray(output);
    }
    _aiter.reset();
    return _result;
}


void TSVInterface::convertChunks(vector< shared_ptr<ConstChunkIterator> > citers, size_t &nCells, string& output)
{
    Value stringVal;
    nCells = 0;
    ostringstream outputBuf;
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
        for (size_t i = 0, n=citers.size(); i < n; ++i)
        {
            Value const& v = citers[i]->getItem();
            if (i || _printCoords)
            {
                outputBuf<<_attDelim;
            }
            if(v.isNull())
            {
                outputBuf<<_nullRepresentation; //TODO: note all missing codes are converted to this representation
            }
            else
            {
                switch(_inputTypes[i])
                {
                case TE_STRING:
                    {
                        char const* s = v.getString();
                        while (char c = *s++)
                        {
                            if (c == '\n')
                            {
                                outputBuf << "\\n";
                            }
                            else if (c == '\t')
                            {
                                outputBuf << "\\t";
                            }
                            else if (c == '\r')
                            {
                                outputBuf << "\\r";
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
                    }
                    break;
                case TE_BOOL:
                    if(v.getBool())
                    {
                        outputBuf<<"true";
                    }
                    else
                    {
                        outputBuf<<"false";
                    }
                    break;
                case TE_DOUBLE:
                    {
                        double nbr =v.getDouble();
                        if(std::isnan(nbr))
                        {
                            outputBuf<<_nanRepresentation;
                        }
                        else
                        {
                            outputBuf.precision(std::numeric_limits<double>::max_digits10);
                            outputBuf<<nbr;
                        }
                    }
                    break;
                case TE_FLOAT:
                    {
                        float fnbr =v.getFloat();
                        if(std::isnan(fnbr))
                        {
                            outputBuf<<_nanRepresentation;
                        }
                        else
                        {
                            outputBuf.precision(std::numeric_limits<float>::max_digits10);
                            outputBuf<<fnbr;
                        }
                    }
                    break;
                case TE_UINT8:
                    {
                        uint8_t nbr =v.getUint8();
                        outputBuf<<(int16_t) nbr;
                    }
                    break;
                case TE_INT8:
                    {
                        int8_t nbr =v.getUint8();
                        outputBuf<<(int16_t) nbr;
                    }
                    break;
                default:
                    {
                        Value const * vv = &v;
                        (*_inputConverters[i])(&vv, &stringVal, NULL);
                        outputBuf<<stringVal.getString();
                    }
                }
            }
        }
        outputBuf<<_lineDelim;
        ++nCells;
        for(size_t i = 0, n=citers.size(); i<n; ++i)
        {
            ++(*citers[i]);
        }
    }
    output = outputBuf.str();
}

void TSVInterface::writeTSV(size_t const nLines, string const& inputData, ChildProcess& child)
{
    LOG4CXX_DEBUG(logger, "Input of stream: "<< inputData);
    char hdr[4096];
    snprintf (hdr, 4096, "%lu\n", nLines);
    size_t n = strlen (hdr);
    child.hardWrite (hdr, n);
    if(n>0)
    {
        child.hardWrite (inputData.c_str(), inputData.size());
    }
}

void TSVInterface::readTSV (std::string& output, ChildProcess& child, bool last)
{
    size_t bufSize = 1024*1024;
    vector<char> buf(bufSize);
    size_t dataSize = child.softRead( &(buf[0]), bufSize, !last);
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
        LOG4CXX_DEBUG(logger, "Got this stuff "<<(&buf[0]));
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
        LOG4CXX_DEBUG(logger, "linesReceived: "<< linesReceived);
        if(linesReceived < expectedNumLines)
        {
            if(idx >= bufSize)
            {
                bufSize = bufSize * 2;
                buf.resize(bufSize);
            }
            dataSize += child.softRead( &(buf[0]) + dataSize, bufSize - dataSize, !last);
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

void TSVInterface::addChunkToArray(string const& output)
{
    shared_ptr<ChunkIterator> citer = _aiter->newChunk(_outPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
    citer->setPosition(_outPos);
    _stringBuf.setString(output);
    citer->writeItem(_stringBuf);
    citer->flush();
    _outPos[1]++;
}

}}
