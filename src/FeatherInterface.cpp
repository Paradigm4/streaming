/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2017 SciDB, Inc.
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

#include <vector>
#include <string>
#include <memory>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>

#include "StreamSettings.h"
#include "ChildProcess.h"
#include "FeatherInterface.h"

using std::vector;
using std::shared_ptr;
using std::string;

namespace scidb { namespace stream {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.stream.feather_interface"));

ArrayDesc FeatherInterface::getOutputSchema(vector<ArrayDesc> const& inputSchemas, Settings const& settings, shared_ptr<Query> const& query)
{
    if(settings.getFormat() != FEATHER)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Feather interface invoked on improper format";
    }
    if(settings.getTypes().size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Feather interface does not support the types parameter";
    }
    if(settings.isChunkSizeSet())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Feather interface does not support the chunk size parameter";
    }
    if(settings.getNames().size() > 1)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Feather interface supports only one result name";
    }
    Dimensions outputDimensions;
    outputDimensions.push_back(DimensionDesc("instance_id", 0,   query->getInstancesCount()-1, 1, 0));
    outputDimensions.push_back(DimensionDesc("chunk_no",    0,   CoordinateBounds::getMax(),   1, 0));
    Attributes outputAttributes;
    outputAttributes.push_back( AttributeDesc(0, settings.getNames().size() ? settings.getNames()[0] : "response",   TID_STRING,    0, 0));
    outputAttributes = addEmptyTagAttribute(outputAttributes);
    return ArrayDesc(inputSchemas[0].getName(), outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
}

FeatherInterface::FeatherInterface(Settings const& settings, ArrayDesc const& outputSchema, std::shared_ptr<Query> const& query):
    _attDelim(  '\t'),
    _lineDelim( '\n'),
    _printCoords(false),
    _nanRepresentation("nan"),
    _nullRepresentation("\\N"),
    _query(query),
    _result(new MemArray(outputSchema, query)),
    _aiter(_result->getIterator(0)),
    _outPos{ ((Coordinate) _query->getInstanceID()), 0}
{}

void FeatherInterface::setInputSchema(ArrayDesc const& inputSchema)
{
    Attributes const& attrs = inputSchema.getAttributes(true);
    size_t const nInputAttrs = attrs.size();
    _inputTypes.resize(nInputAttrs);
    _inputNames.resize(nInputAttrs);
    _inputConverters.resize(nInputAttrs);
    for(size_t i=0; i < nInputAttrs; ++i)
    {
        TypeId const& inputType = attrs[i].getType();
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
        _inputNames[i]= attrs[i].getName();
    }
}

void FeatherInterface::streamData(std::vector<ConstChunk const*> const& inputChunks,
                                  ChildProcess& child)
{
    if(inputChunks.size() != _inputTypes.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
          << "received inconsistent number of input chunks";
    }
    size_t numRows = inputChunks[0]->count();
    if(numRows == 0)
    {
        return;
    }
    if(numRows > (size_t) std::numeric_limits<int32_t>::max())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
          << "received chunk with count exceeding the Arrow array limit";
    }
    if(!child.isAlive())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
          << "child exited early";
    }
    writeFeather(inputChunks, numRows, child);
    readFeather(child);
}

shared_ptr<Array> FeatherInterface::finalize(ChildProcess& child)
{
    writeFinalFeather(child);
    readFeather(child, true);
    _aiter.reset();
    return _result;
}

void FeatherInterface::writeFeather(vector<ConstChunk const*> const& chunks,
                                    int32_t const numRows,
                                    ChildProcess& child)
{
    int32_t numColumns = chunks.size();
    LOG4CXX_DEBUG(logger, "writeFeather::numColumns:" << numColumns);
    LOG4CXX_DEBUG(logger, "writeFeather::numRows:" << numRows);

    std::shared_ptr<arrow::io::BufferOutputStream> stream;
    arrow::io::BufferOutputStream::Create(
        1024, arrow::default_memory_pool(), &stream);

    std::unique_ptr<arrow::ipc::feather::TableWriter> writer;
    arrow::ipc::feather::TableWriter::Open(stream, &writer);

    writer->SetNumRows(numRows);

    for(size_t i = 0; i < _inputTypes.size(); ++i)
    {
        shared_ptr<ConstChunkIterator> citer =
          chunks[i]->getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS
                                      | ConstChunkIterator::IGNORE_EMPTY_CELLS);

        std::shared_ptr<arrow::ArrayBuilder> builder;
        switch(_inputTypes[i])
        {
        case TE_INT64:
            {
                builder.reset(
                    new arrow::Int64Builder(
                        arrow::default_memory_pool(),
                        arrow::int64()));
            }
            break;
        }
        while((!citer->end()))
        {
            Value const& value = citer->getItem();
            switch(_inputTypes[i])
            {
            case TE_INT64:
                {
                    if(value.isNull())
                    {
                        std::dynamic_pointer_cast<arrow::Int64Builder>(
                            builder)->AppendNull();
                    }
                    else
                    {
                        std::dynamic_pointer_cast<arrow::Int64Builder>(
                            builder)->Append(value.getInt64());
                    }
                }
                break;
            }
            ++(*citer);
        }

        std::shared_ptr<arrow::Array> array;
        builder->Finish(&array);

        // Print array for debugging
        std::stringstream prettyprint_stream;
        arrow::PrettyPrint(*array, 0, &prettyprint_stream);
        LOG4CXX_DEBUG(logger, "writeFeather::array:"
                      << prettyprint_stream.str().c_str());

        writer->Append(_inputNames[i].c_str(), *array);
    }
    writer->Finalize();

    std::shared_ptr<arrow::Buffer> buffer;
    stream->Finish(&buffer);

    int64_t sz = buffer->size();
    LOG4CXX_DEBUG(logger, "writeFeather::sizeBuffer:" << sz);
    child.hardWrite(&sz, sizeof(int64_t));
    child.hardWrite(buffer->data(), buffer->size());
}

void FeatherInterface::writeFinalFeather(ChildProcess& child)
{
    LOG4CXX_DEBUG(logger, "writeFinalFeather::0");

    int64_t zero = 0;
    child.hardWrite(&zero, sizeof(int64_t));
}

void FeatherInterface::readFeather(ChildProcess& child,
                                   bool lastMessage)
{
    LOG4CXX_DEBUG(logger, "readFeather");

    int64_t flag;
    child.hardRead(&flag, sizeof(int64_t), !lastMessage);
    LOG4CXX_DEBUG(logger, "readFeather::flag:" << flag);

    shared_ptr<ChunkIterator> citer =
      _aiter->newChunk(_outPos).getIterator(_query,
                                            ChunkIterator::SEQUENTIAL_WRITE);
    citer->setPosition(_outPos);
    _stringBuf.setString("Hi!");
    citer->writeItem(_stringBuf);
    citer->flush();
    _outPos[1]++;
}

}}
