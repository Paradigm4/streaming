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

#include "DFInterface.h"
#include "StreamSettings.h"
#include "ChildProcess.h"
#include <vector>
#include <string>
#include <query/Query.h>
#include <array/MemArray.h>

using std::vector;
using std::shared_ptr;
using std::string;

namespace scidb { namespace stream {

ArrayDesc DFInterface::getOutputSchema(std::vector<ArrayDesc> const& inputSchemas, Settings const& settings, std::shared_ptr<Query> const& query)
{
    if(settings.getFormat() != DF)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "DF interface invoked on improper format";
    }
    vector<TypeEnum> outputTypes = settings.getTypes();
    vector<string>   outputNames = settings.getNames();
    if(outputTypes.size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "DF interface requires that output types are specified";
    }
    if(outputNames.size() == 0)
    {
        for(size_t i =0; i<outputTypes.size(); ++i)
        {
            ostringstream name;
            name<<"a"<<i;
            outputNames.push_back(name.str());
        }
    }
    else if (outputNames.size() != outputTypes.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received inconsistent names and types";
    }
    for(size_t i = 0; i<inputSchemas.size(); ++i)
    {
        ArrayDesc const& schema = inputSchemas[i];
        Attributes const& attrs = schema.getAttributes(true);
            //for(size_t j = 0, nAttrs = attrs.size(); j<nAttrs; ++j)
        for (const auto& attr : attrs)
        {
//            AttributeDesc const& attr = attrs[j];
            TypeEnum te = typeId2TypeEnum(attr.getType(), true);
            if(te != TE_UINT16 &&  te != TE_INT32 && te != TE_DOUBLE && te != TE_STRING)
            {
                ostringstream error;
                error<<"Attribute "<<attr.getName()<<" has unsupported type "<<attr.getType()<<" only double, uint16, int32 and string supported right now";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }
        }
    }
    Dimensions outputDimensions;
    outputDimensions.push_back(DimensionDesc("instance_id", 0,   query->getInstancesCount()-1, 1, 0));
    outputDimensions.push_back(DimensionDesc("chunk_no",    0,   CoordinateBounds::getMax(),   1, 0));
    outputDimensions.push_back(DimensionDesc("value_no",    0,   CoordinateBounds::getMax(),   settings.getChunkSize(), 0));
    Attributes outputAttributes;
    for(AttributeID i =0; i<outputTypes.size(); ++i)
    {
        outputAttributes.push_back( AttributeDesc(outputNames[i], typeEnum2TypeId(outputTypes[i]), AttributeDesc::IS_NULLABLE, CompressorType::NONE));
    }
    outputAttributes.addEmptyTagAttribute();
    return ArrayDesc(inputSchemas[0].getName(), outputAttributes, outputDimensions, createDistribution(defaultDistType()), query->getDefaultArrayResidency());
}

DFInterface::DFInterface(Settings const& settings, ArrayDesc const& outputSchema, std::shared_ptr<Query> const& query):
    _query(query),
    _result(new MemArray(outputSchema, query)),
    _outPos{ ((Coordinate) query->getInstanceID()), 0, 0 },
    _outputChunkSize(settings.getChunkSize()),
    _nOutputAttrs( (int32_t) outputSchema.getAttributes(true).size()),
    _oaiters(_nOutputAttrs+1),
    _outputTypes(_nOutputAttrs),
    _readBuf(1024*1024),
    _writeBuf(1024*1024)
{
//    for(int32_t i =0; i<_nOutputAttrs; ++i)
    int32_t i =0;
    for (const auto& attr : outputSchema.getAttributes(true))
    {
        _oaiters[i] = _result->getIterator(attr);
        _outputTypes[i] = settings.getTypes()[i];
        i++;
    }
    _oaiters[_nOutputAttrs] = _result->getIterator(*outputSchema.getEmptyBitmapAttribute());
    _nullVal.setNull();
    unsigned char nanDouble[8] = { 0xa2, 0x07, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x7f };
    _rNanDouble = *((double*) (&nanDouble));
    _rNanInt32  = std::numeric_limits<int32_t>::min();
}

void DFInterface::setInputSchema(ArrayDesc const& inputSchema)
{
    Attributes const& attrs = inputSchema.getAttributes(true);
    size_t const nInputAttrs = attrs.size();
    _inputTypes.resize(nInputAttrs);
    _inputNames.resize(nInputAttrs);
//    for(size_t i =0; i<nInputAttrs; ++i)
    size_t i =0;
    for (const auto& attr : attrs)
    {
        _inputTypes[i]= typeId2TypeEnum(attr.getType());
        _inputNames[i]= attr.getName();
        i++;
    }
}

void DFInterface::streamData(std::vector<ConstChunk const*> const& inputChunks, ChildProcess& child)
{
    if(inputChunks.size() != _inputTypes.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "inconsistent input chunks given";
    }
    size_t nRows = inputChunks[0]->count();
    if(nRows == 0)
    {
        return;
    }
    if(nRows > (size_t) std::numeric_limits<int32_t>::max())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received chunk with count exceeding the R vector limit";
    }
    if(!child.isAlive())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "child exited early";
    }
    writeDF(inputChunks, nRows, child);
    readDF(child);
}

shared_ptr<Array> DFInterface::finalize(ChildProcess& child)
{
    writeFinalDF(child);
    readDF(child, true);
    _oaiters.clear();
    return _result;
}

static const unsigned char R_HEADER[14]    = { 0x42, 0x0a, 0x02, 0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x03, 0x02, 0x00 };
static const unsigned char R_EVECSXP[4]    = { 0x13, 0x00, 0x00, 0x00 };     // R list without attributes
static const unsigned char R_VECSXP[4]     = { 0x13, 0x02, 0x00, 0x00 };     // R list with attributes
static const unsigned char R_INTSXP[4]     = { 0x0d, 0x00, 0x00, 0x00 };
static const unsigned char R_REALSXP[4]    = { 0x0e, 0x00, 0x00, 0x00 };
static const unsigned char R_CHARSXP[4]    = { 0x09, 0x00, 0x04, 0x00 };    // UTF-8
static const unsigned char R_STRSXP[4]     = { 0x10, 0x00, 0x00, 0x00 };
static const unsigned char R_LISTSXP[4]    = { 0x02, 0x04, 0x00, 0x00 };    // internal R pairlist
static const unsigned char R_TAIL_HDR[21]  = { 0x02, 0x04, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x09, 0x00, 0x04, 0x00, 0x05, 0x00, 0x00, 0x00, 0x6e, 0x61, 0x6d, 0x65, 0x73 };
static const unsigned char R_TAIL[4]       = { 0xfe, 0x00, 0x00, 0x00 };

void DFInterface::writeDF(vector<ConstChunk const*> const& chunks, int32_t const numRows, ChildProcess& child)
{
    child.hardWrite(R_HEADER, sizeof(R_HEADER));
    child.hardWrite(R_VECSXP, sizeof(R_VECSXP));
    int32_t numColumns = chunks.size();
    child.hardWrite(&numColumns, sizeof(int32_t));
    for(size_t i =0; i<_inputTypes.size(); ++i)
    {
        switch(_inputTypes[i])
        {
        case TE_STRING:     child.hardWrite (R_STRSXP,  sizeof (R_STRSXP));  break;
        case TE_DOUBLE:     child.hardWrite (R_REALSXP, sizeof (R_REALSXP)); break;
        case TE_UINT16:
        case TE_INT32:      child.hardWrite (R_INTSXP,  sizeof (R_INTSXP));  break;
        default:         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: unknown type";
        }
        child.hardWrite(&numRows, sizeof(int32_t));
        shared_ptr<ConstChunkIterator> citer = chunks[i]->getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
        _writeBuf.reset();
        while((!citer->end()))
        {
            Value const& v = citer->getItem();
            switch(_inputTypes[i])
            {
            case TE_STRING:
            {
                _writeBuf.pushData(&R_CHARSXP, sizeof(R_CHARSXP));
                if(v.isNull())
                {
                    int32_t size = -1;
                    _writeBuf.pushData(&size, sizeof(int32_t));
                }
                else
                {
                    int32_t size = v.size() - 1;
                    _writeBuf.pushData(&size, sizeof(int32_t));
                    _writeBuf.pushData(v.getString(), size);
                }
                break;
            }
            case TE_DOUBLE:
            {
                if(v.isNull())
                {
                    _writeBuf.pushData(&_rNanDouble, sizeof(double));
                }
                else
                {
                    double  datum = v.getDouble();
                    _writeBuf.pushData(&datum, sizeof(double));
                }
                break;
            }
            case TE_UINT16:
            {
                if(v.isNull())
                {
                    _writeBuf.pushData(&_rNanInt32, sizeof(int32_t));
                }
                else
                {
                    int32_t datum = (int32_t) (v.getUint16());
                    _writeBuf.pushData(&datum, sizeof(int32_t));
                }
                break;
            }
            case TE_INT32:
            {
                if(v.isNull())
                {
                    _writeBuf.pushData(&_rNanInt32, sizeof(int32_t));
                }
                else
                {
                    int32_t datum = v.getInt32();
                    _writeBuf.pushData(&datum, sizeof(int32_t));
                }
                break;
            }
            default: throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: unsupported type";
            }
            ++(*citer);
        }
        child.hardWrite(_writeBuf.data(), _writeBuf.size());
    }
    child.hardWrite(R_TAIL_HDR, sizeof(R_TAIL_HDR));
    child.hardWrite(R_STRSXP, sizeof(R_STRSXP));
    child.hardWrite(&numColumns, sizeof(int32_t));
    for(size_t i =0; i<_inputTypes.size(); ++i)
    {
        child.hardWrite(R_CHARSXP, sizeof(R_CHARSXP));
        int32_t nameSize = _inputNames[i].size();
        child.hardWrite(&nameSize, sizeof(int32_t));
        child.hardWrite(_inputNames[i].c_str(), nameSize);
    }
    child.hardWrite(R_TAIL, sizeof(R_TAIL));
}

void DFInterface::writeFinalDF(ChildProcess& child)
{
    child.hardWrite(R_HEADER,  sizeof(R_HEADER));
    child.hardWrite(R_EVECSXP, sizeof(R_VECSXP));
    int32_t numColumns = 0;
    child.hardWrite(&numColumns, sizeof(int32_t));
}

void DFInterface::readDF(ChildProcess& child, bool lastMessage)
{
    child.hardRead(&(_readBuf[0]), sizeof(R_HEADER) + sizeof(R_VECSXP), !lastMessage);
    int32_t intBuf;
    int32_t numColumns = -1;
    child.hardRead(&numColumns, sizeof(int32_t), !lastMessage);
    if (numColumns > 0 && numColumns != _nOutputAttrs)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received incorrect number of columns "; // << numColumns << " attrs " << _nOutputAttrs;
    }
    if (numColumns == 0)
    {
        return;
    }
    int32_t numRows;
    for(int32_t i =0; i<numColumns; ++i)
    {
        switch(_outputTypes[i])
        {
        case TE_STRING:     child.hardRead (&(_readBuf[0]),  sizeof (R_STRSXP),  !lastMessage);  break;
        case TE_DOUBLE:     child.hardRead (&(_readBuf[0]),  sizeof (R_REALSXP), !lastMessage);  break;
        case TE_INT32:      child.hardRead (&(_readBuf[0]),  sizeof (R_INTSXP),  !lastMessage);  break;
        default:         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: unknown type";
        }
        if( i == 0)
        {
            child.hardRead(&numRows, sizeof(int32_t));
            if(numRows < 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received negative number of rows";
            }
        }
        else
        {
            child.hardRead(&intBuf, sizeof(int32_t));
            if(intBuf != numRows)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received lists of different sizes";
            }
        }
        if(numRows == 0)
        {
            continue;
        }
        switch(_outputTypes[i])
        {
        case TE_DOUBLE:
        {
            size_t readSize = sizeof(double) * numRows;
            if(readSize > _readBuf.size())
            {
                _readBuf.resize(readSize);
            }
            child.hardRead (&(_readBuf[0]), readSize, !lastMessage);
            break;
        }
        case TE_INT32:
        {
            size_t readSize = sizeof(int32_t) * numRows;
            if(readSize > _readBuf.size())
            {
                _readBuf.resize(readSize);
            }
            child.hardRead (&(_readBuf[0]), readSize, !lastMessage);
            break;
        }
        case TE_STRING:
        {
            break; // all below
        }
        default:         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: unknown type";
        }
        shared_ptr<ChunkIterator> ociter = _oaiters[i]->newChunk(_outPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE  | ChunkIterator::NO_EMPTY_CHECK );
        Coordinates valPos = _outPos;
        for(int32_t j = 0; j<numRows; ++j)
        {
            ociter->setPosition(valPos);
            switch(_outputTypes[i])
            {
            case TE_STRING:
            {
                child.hardRead(&(_readBuf[0]), sizeof(R_CHARSXP) + sizeof(int32_t), !lastMessage);
                int32_t size = *((int32_t*) (&(_readBuf[0]) + sizeof(R_CHARSXP)));
                if(size<-1)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "error reading string size";
                }
                if(size == -1)
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    if( (size_t) size+1 > _readBuf.size())
                    {
                        _readBuf.resize(size+1);
                    }
                    child.hardRead(&(_readBuf[0]), size, !lastMessage);
                    _readBuf[size] = 0;
                    _val.setData( &(_readBuf[0]), size+1);
                    ociter->writeItem(_val);
                }
                break;
            }
            case TE_DOUBLE:
            {
                double v = ((double*)(&(_readBuf[0])))[j];
                if( memcmp(&v, &_rNanDouble, sizeof(double))==0)
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    _val.setDouble(v);
                    ociter->writeItem(_val);
                }
                break;
            }
            case TE_INT32:
            {
                int32_t v = ((int32_t*)(&(_readBuf[0])))[j];
                if (v == _rNanInt32)
                {
                    ociter->writeItem(_nullVal);
                }
                else
                {
                    _val.setInt32(v);
                    ociter->writeItem(_val);
                }
                break;
            }
            default:         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal error: unknown type";
            }
            ++valPos[2];
        }
        ociter->flush();
    }
    if(numRows != 0)
    {
        Value bmVal;
        bmVal.setBool(true);                //populate the empty tag
        shared_ptr<ChunkIterator> bmCiter = _oaiters[_nOutputAttrs]->newChunk(_outPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE  | ChunkIterator::NO_EMPTY_CHECK );
        Coordinates valPos = _outPos;
        for(int32_t j =0; j<numRows; ++j)
        {
            bmCiter->setPosition(valPos);
            bmCiter->writeItem(bmVal);
            ++valPos[2];
        }
        bmCiter->flush();
        _outPos[1]++;
    }
    child.hardRead(&(_readBuf[0]), sizeof(R_TAIL_HDR) + sizeof(R_STRSXP) + sizeof(int32_t), !lastMessage);
    for(int32_t i =0; i<numColumns; ++i)
    {
        child.hardRead(&(_readBuf[0]), sizeof(R_CHARSXP), !lastMessage);
        child.hardRead(&intBuf, sizeof(int32_t), !lastMessage);
        child.hardRead(&(_readBuf[0]), intBuf, !lastMessage);
    }
    child.hardRead(&(_readBuf[0]), sizeof(R_TAIL), !lastMessage);
}

}}
