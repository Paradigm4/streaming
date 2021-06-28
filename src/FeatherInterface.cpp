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

#include "StreamSettings.h"
#include "ChildProcess.h"
#include "FeatherInterface.h"

#include <array/MemArray.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

#define THROW_NOT_OK(s)                                                 \
    {                                                                   \
        arrow::Status _s = (s);                                         \
        if (!_s.ok())                                                   \
        {                                                               \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)      \
                    << _s.ToString().c_str();                           \
        }                                                               \
    }

#define ASSIGN_OR_THROW(lhs, rexpr)                     \
    {                                                   \
        auto status_name = (rexpr);                     \
        THROW_NOT_OK(status_name.status());             \
        lhs = std::move(status_name).ValueOrDie();      \
    }

namespace scidb { namespace stream {

ArrayDesc FeatherInterface::getOutputSchema(
    std::vector<ArrayDesc> const& inputSchemas,
    Settings const& settings,
    std::shared_ptr<Query> const& query)
{
    if(settings.getFormat() != FEATHER)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << "FEATHER interface invoked on improper format";
    }
    vector<TypeEnum> outputTypes = settings.getTypes();
    vector<string>   outputNames = settings.getNames();
    if(outputTypes.size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << "FEATHER interface requires that output types are specified";
    }
    if(outputNames.size() == 0)
    {
        for(size_t i =0; i<outputTypes.size(); ++i)
        {
            ostringstream name;
            name << "a" << i;
            outputNames.push_back(name.str());
        }
    }
    else if (outputNames.size() != outputTypes.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << "received inconsistent names and types";
    }
    for(size_t i = 0; i<inputSchemas.size(); ++i)
    {
        ArrayDesc const& schema = inputSchemas[i];
        Attributes const& attrs = schema.getAttributes(true);
//        for(size_t j = 0, nAttrs = attrs.size(); j<nAttrs; ++j)
        for(AttributeDesc const&attr : attrs)
        {
            TypeEnum te = typeId2TypeEnum(attr.getType(), true);
        }
    }
    Dimensions outputDimensions;
    outputDimensions.push_back(
        DimensionDesc(
            "instance_id", 0, query->getInstancesCount() - 1, 1, 0));
    outputDimensions.push_back(
        DimensionDesc(
            "chunk_no", 0, CoordinateBounds::getMax(), 1, 0));
    outputDimensions.push_back(DimensionDesc("value_no",
                                             0,
                                             CoordinateBounds::getMax(),
                                             settings.getChunkSize(),
                                             0));
    Attributes outputAttributes;
    for(AttributeID i =0; i<outputTypes.size(); ++i)
    {
        outputAttributes.push_back(
            AttributeDesc(outputNames[i],
                          typeEnum2TypeId(outputTypes[i]),
                          AttributeDesc::IS_NULLABLE, CompressorType::NONE));
    }
    outputAttributes.addEmptyTagAttribute();
    return ArrayDesc(inputSchemas[0].getName(),
                     outputAttributes,
                     outputDimensions,
                     createDistribution(dtUndefined),
                     query->getDefaultArrayResidency());
}

FeatherInterface::FeatherInterface(Settings const& settings,
                                   ArrayDesc const& outputSchema,
                                   std::shared_ptr<Query> const& query):
    _query(query),
    _result(new MemArray(outputSchema, query)),
    _outPos{((Coordinate) _query->getInstanceID()), 0, 0},
    _outputChunkSize(settings.getChunkSize()),
    _nOutputAttrs((int32_t)outputSchema.getAttributes(true).size()),
    _oaiters(_nOutputAttrs + 1),
    _outputTypes(settings.getTypes()),
    _readBuf(1024*1024)
{
    // Set output iterators
    size_t i = 0;
    for (const auto& attr : outputSchema.getAttributes(true))
    {
        _oaiters[i] = _result->getIterator(attr);
        i++;
    }
    _oaiters[_nOutputAttrs] = _result->getIterator(
        *outputSchema.getEmptyBitmapAttribute());
    _nullVal.setNull();
}

void FeatherInterface::setInputSchema(ArrayDesc const& inputSchema)
{
    Attributes const& attrs = inputSchema.getAttributes(true);
    size_t const nInputAttrs = attrs.size();

    _inputTypes.resize(nInputAttrs);
    _inputArrowBuilders.resize(nInputAttrs);
    std::vector<std::shared_ptr<arrow::Field>> arrowFields(nInputAttrs);

    size_t i = 0;
    for (const auto& attr : attrs)
    {
        TypeId const& inputType = attr.getType();
        _inputTypes[i] = typeId2TypeEnum(inputType, true);

        std::shared_ptr<arrow::DataType> arrowType;
        auto scidbType = _inputTypes[i];
        switch (scidbType) {
        case TE_BINARY: {
            arrowType = arrow::binary();
            break;
        }
        case TE_DOUBLE: {
            arrowType = arrow::float64();
            break;
        }
        case TE_INT64: {
            arrowType = arrow::int64();
            break;
        }
        case TE_STRING: {
            arrowType = arrow::utf8();
            break;
        }
        default: {
            std::ostringstream error;
            error << "Type " << scidbType << " not supported by Stream plug-in";
            throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                   SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        }
        arrowFields[i] = arrow::field(
            attr.getName(), arrowType, false); // attr.isNullable()

        THROW_NOT_OK(
            arrow::MakeBuilder(_arrowPool, arrowType, &_inputArrowBuilders[i]));

        i++;
    }

    _inputArrowSchema = arrow::schema(arrowFields);
}

void FeatherInterface::streamData(
    std::vector<ConstChunk const*> const& inputChunks,
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
    THROW_NOT_OK(writeFeather(inputChunks, numRows, child));
    readFeather(child);
}

shared_ptr<Array> FeatherInterface::finalize(ChildProcess& child)
{
    writeFinalFeather(child);
    readFeather(child, true);
    _oaiters.clear();
    return _result;
}

arrow::Status FeatherInterface::writeFeather(vector<ConstChunk const*> const& chunks,
                                    int32_t const numRows,
                                    ChildProcess& child)
{
    size_t numColumns = chunks.size();
    if (numColumns != _inputTypes.size()) {
        std::ostringstream error;
        error << "Number of chunks " << numColumns
              << " does not match number of input types " << _inputTypes.size();
        throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                               SCIDB_LE_ILLEGAL_OPERATION) << error.str();
    }

    LOG4CXX_DEBUG(logger, "stream|" << _query->getInstanceID()
                  << "|write|numColumns: " << numColumns
                  << ", numRows: " << numRows);

    std::vector<std::shared_ptr<arrow::Array>> arrowArrays(numColumns);
    for(size_t i = 0; i < numColumns; ++i)
    {
        shared_ptr<ConstChunkIterator> citer =
            chunks[i]->getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);

        auto scidbType = _inputTypes[i];
        switch(scidbType)
        {
        case TE_INT64:
        {
            while((!citer->end()))
            {
                Value const& value = citer->getItem();
                if(value.isNull())
                {
                    _inputArrowBuilders[i]->AppendNull();
                }
                else
                {
                    static_cast<arrow::Int64Builder*>(
                        _inputArrowBuilders[i].get())->Append(value.getInt64());
                }
                ++(*citer);
            }
            break;
        }
        case TE_DOUBLE:
        {
            while((!citer->end()))
            {
                Value const& value = citer->getItem();
                if(value.isNull())
                {
                    _inputArrowBuilders[i]->AppendNull();
                }
                else
                {
                    static_cast<arrow::DoubleBuilder*>(
                        _inputArrowBuilders[i].get())->Append(value.getDouble());
                }
                ++(*citer);
            }
            break;
        }
        case TE_STRING:
        {
            while((!citer->end()))
            {
                Value const& value = citer->getItem();
                if(value.isNull())
                {
                    _inputArrowBuilders[i]->AppendNull();
                }
                else
                {
                    static_cast<arrow::StringBuilder*>(
                        _inputArrowBuilders[i].get())->Append(value.getString());
                }
                ++(*citer);
            }
            break;
        }
        case TE_BINARY:
        {
            while((!citer->end()))
            {
                Value const& value = citer->getItem();
                if(value.isNull())
                {
                    _inputArrowBuilders[i]->AppendNull();
                }
                else
                {
                    static_cast<arrow::BinaryBuilder*>(
                        _inputArrowBuilders[i].get())->Append(
                            (const uint8_t*)value.data(), value.size());
                }
                ++(*citer);
            }
            break;
        }
        default:
        {
            std::ostringstream error;
            error << "Type " << scidbType << " not supported by Stream plug-in";
            throw SYSTEM_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                   SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        }
    }

    // Finalize Arrow Builders and write Arrow Arrays (resets builders)
    for (size_t i = 0; i < numColumns; ++i)
        // Resets Builder!
        THROW_NOT_OK(_inputArrowBuilders[i]->Finish(&arrowArrays[i]));

    // Create Arrow Record Batch
    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    arrowBatch = arrow::RecordBatch::Make(
        _inputArrowSchema, numRows, arrowArrays);
    ARROW_RETURN_NOT_OK(arrowBatch->Validate());

    // Stream Arrow Record Batch to Arrow Buffer using Arrow
    // Record Batch Writer and Arrow Buffer Output Stream
    std::shared_ptr<arrow::io::BufferOutputStream> arrowBufferStream;
    ARROW_ASSIGN_OR_RAISE(
        arrowBufferStream,
        // TODO Better initial estimate for Create
        arrow::io::BufferOutputStream::Create(4096, _arrowPool));

    // Setup Arrow Compression, If Enabled
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
    ASSIGN_OR_THROW(
        arrowWriter,
        arrow::ipc::MakeStreamWriter(&*arrowBufferStream, _inputArrowSchema));

    ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*arrowBatch));
    ARROW_RETURN_NOT_OK(arrowWriter->Close());

    std::shared_ptr<arrow::Buffer> arrowBuffer;
    ARROW_ASSIGN_OR_RAISE(arrowBuffer, arrowBufferStream->Finish());

    uint64_t writeSize = arrowBuffer->size();
    LOG4CXX_DEBUG(logger, "stream|" << _query->getInstanceID()
                  << "|write|writeSize: " << writeSize);
    child.hardWrite(&writeSize, sizeof(uint64_t));
    child.hardWrite(arrowBuffer->data(), writeSize);

    return arrow::Status::OK();
}

void FeatherInterface::writeFinalFeather(ChildProcess& child)
{
    LOG4CXX_DEBUG(logger, "stream|" << _query->getInstanceID() << "|writeFinal");

    int64_t zero = 0;
    child.hardWrite(&zero, sizeof(int64_t));
}

void FeatherInterface::readFeather(ChildProcess& child, bool lastMessage)
{
    uint64_t readSize;
    child.hardRead(&readSize, sizeof(uint64_t), !lastMessage);
    LOG4CXX_DEBUG(logger, "stream|" << _query->getInstanceID()
                  << "|read|readSize: " << readSize);
    if (readSize == 0)
    {
        return;
    }

    if (readSize > _readBuf.size())
    {
        _readBuf.resize(readSize);
    }
    child.hardRead(&(_readBuf[0]), readSize, !lastMessage);

    arrow::Buffer arrowBuffer(&(_readBuf[0]), readSize);
    auto arrowBufferReader = std::make_shared<arrow::io::BufferReader>(
            arrowBuffer);

    std::shared_ptr<arrow::RecordBatchReader> arrowBatchReader;
    ASSIGN_OR_THROW(
            arrowBatchReader,
            arrow::ipc::RecordBatchStreamReader::Open(arrowBufferReader));

    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    THROW_NOT_OK(arrowBatchReader->ReadNext(&arrowBatch));

    // No More Record Batches are Expected
    std::shared_ptr<arrow::RecordBatch> arrowBatchNext;
    THROW_NOT_OK(arrowBatchReader->ReadNext(&arrowBatchNext));
    if (arrowBatchNext != NULL)
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_ARRAY_WRITER,
            SCIDB_LE_UNKNOWN_ERROR)
            << "More than one Arrow Record Batch found in response from client";

    int64_t numColumns = arrowBatch->num_columns();
    int64_t numRows = arrowBatch->num_rows();
    LOG4CXX_DEBUG(logger, "stream|" << _query->getInstanceID()
                  << "|read|numColumns:" << numColumns
                  << ", numRows:" << numRows);

    if (numColumns > 0 && numColumns != _nOutputAttrs)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << "received incorrect number of columns";
    }
    if (numColumns == 0 || numRows == 0)
    {
        return;
    }

    std::shared_ptr<arrow::ChunkedArray> col;
    for(int64_t i = 0; i < numColumns; ++i)
    {
        std::shared_ptr<arrow::Array> array = arrowBatch->column(i);
        int64_t nullCount = array->null_count();
        const uint8_t* nullBitmap = array->null_bitmap_data();

        shared_ptr<ChunkIterator> ociter = _oaiters[i]->newChunk(
            _outPos).getIterator(_query,
                                 ChunkIterator::SEQUENTIAL_WRITE
                                 | ChunkIterator::NO_EMPTY_CHECK);
        Coordinates valPos = _outPos;

        switch(_outputTypes[i])
        {
        case TE_INT64:
        {
            const int64_t* arrayData =
                arrowBatch->column_data(i)->GetValues<int64_t>(1);

            for(int64_t j = 0; j < numRows; ++j)
            {
                ociter->setPosition(valPos);
                if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    _val.setInt64(arrayData[j]);
                    ociter->writeItem(_val);
                }
                ++valPos[2];
            }
            break;
        }
        case TE_DOUBLE:
        {
            const double* arrayData =
                arrowBatch->column_data(i)->GetValues<double>(1);

            for(int64_t j = 0; j < numRows; ++j)
            {
                ociter->setPosition(valPos);
                if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    _val.setDouble(arrayData[j]);
                    ociter->writeItem(_val);
                }
                ++valPos[2];
            }
            break;
        }
        case TE_STRING:
        {
            std::shared_ptr<arrow::StringArray> arrayString =
                std::static_pointer_cast<arrow::StringArray>(array);

            for(int64_t j = 0; j < numRows; ++j)
            {
                ociter->setPosition(valPos);
                if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    // Strings in Arrow arrays are not null-terminated
                    _val.setString(arrayString->GetString(j));
                    ociter->writeItem(_val);
                }
                ++valPos[2];
            }
            break;
        }
        case TE_BINARY:
        {
            std::shared_ptr<arrow::BinaryArray> arrayBinary =
                std::static_pointer_cast<arrow::BinaryArray>(array);

            for(int64_t j = 0; j < numRows; ++j)
            {
                ociter->setPosition(valPos);
                if (nullCount != 0 && ! (nullBitmap[j / 8] & 1 << j % 8))
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    const uint8_t* ptr_val;
                    int32_t sz_val;
                    ptr_val = arrayBinary->GetValue(j, &sz_val);
                    _val.setData(ptr_val, sz_val);
                    ociter->writeItem(_val);
                }
                ++valPos[2];
            }
            break;
        }
        default: throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                        SCIDB_LE_ILLEGAL_OPERATION)
            << "internal error: unknown type";
        }
        ociter->flush();
    }

    // populate the empty tag
    Value bmVal;
    bmVal.setBool(true);
    shared_ptr<ChunkIterator> bmCiter = _oaiters[_nOutputAttrs]->newChunk(
        _outPos).getIterator(_query,
                             ChunkIterator::SEQUENTIAL_WRITE
                             | ChunkIterator::NO_EMPTY_CHECK);
    Coordinates valPos = _outPos;
    for(int64_t j =0; j<numRows; ++j)
    {
        bmCiter->setPosition(valPos);
        bmCiter->writeItem(bmVal);
        ++valPos[2];
    }
    bmCiter->flush();
    _outPos[1]++;
}

}}
