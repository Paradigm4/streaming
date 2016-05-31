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

#ifndef SRC_DFINTERFACE_H_
#define SRC_DFINTERFACE_H_

#include <query/Operator.h>
#include <query/TypeSystem.h>

namespace scidb { namespace stream
{

class Settings;
class ChildProcess;

/**
 * Interface for streaming data in R data.frame format (abbreviated DF below).Converts SciDB data to DF and
 * communicates with the child process. To be precise the data is transferred as an R list of named vectors, whose
 * lengths must match. For example, a three-attribute message looks like this:
 *
 * list( a0=c(1.234, 5.678, 9.012), a1=as.integer(c(3,4,5)), a2=c('alex', 'bob', 'clark'))
 *
 * An empty message contains 0 columns, created like this:
 *
 * list()
 *
 * Only 3 datatypes are supported: string, double, int32. All SciDB null codes convert to R NA values for
 * these types. In reverse, R NA values are converted to SciDB null (code 0).
 */
class DFInterface
{
public:
    // General streaming interface methods //

    /**
     * Determine the output array schema returned by this interface.
     * @param inputSchemas the schenas of the input arrays that will be supplied
     * @param settings the settings of the operator
     * @param query the query context
     * @return a schema of the array that a subsequent finalize call will produce with these parameters
     */
    static ArrayDesc getOutputSchema(std::vector<ArrayDesc> const& inputSchemas, Settings const& settings, std::shared_ptr<Query> const& query);

    /**
     * Create the interface.
     * @param settings the settings of the operator
     * @param outputSchema must be the result of a previous getOutputSchema call for these settings
     * @param query the query context
     */
    DFInterface(Settings const& settings, ArrayDesc const& outputSchema, std::shared_ptr<Query> const& query);

    /**
     * Set the interface to stream chunks from a given array. Must be called before streamData, when first
     * starting to stream and whenever the array that chunks are streamed from changes
     * @param inputSchema the schema of the array whose chunks will be streamed
     */
    void setInputSchema(ArrayDesc const& inputSchema);

    /**
     * Write data to the child and record the response into an internal array.
     * @param inputChunks the data must match the attributes from the most recent setInputSchema call,
     *                    excluding the empty tag.
     * @param child the process to stream to
     */
    void streamData(std::vector<ConstChunk const*> const& inputChunks, ChildProcess& child);

    /**
     * Finish the interaction, write the terminating message to the child and return a pointer to the array
     * containing all the accumulated result data. This object is invalidated after this call.
     * @param child the process to stream to
     * @return the array containing the result of the entire streaming session
     */
    std::shared_ptr<Array> finalize(ChildProcess& child);

private:
    class EasyBuffer
    {
    private:
        std::vector<char> _data;
        size_t       _end;

    public:
        EasyBuffer(size_t initialCapacity = 1024*1024):
            _data(initialCapacity),
            _end(0)
        {}

        void pushData(void const* data, size_t size)
        {
            if(_end + size > _data.size())
            {
                _data.resize(_end + size);
            }
            memcpy((&(_data[_end])), data, size);
            _end += size;
        }

        void reset()
        {
            _end   = 0;
        }

        /**
         * Result of this call is invalidated after next pushData call
         */
        void* data()
        {
            return (&_data[0]);
        }

        size_t size()
        {
            return _end;
        }
    };

    std::shared_ptr<Query>                         _query;
    std::shared_ptr<Array>                         _result;
    Coordinates                                    _outPos;
    size_t                                         _outputChunkSize;
    int32_t                                        _nOutputAttrs;
    std::vector< std::shared_ptr<ArrayIterator> >  _oaiters;
    std::vector <TypeEnum>                         _outputTypes;
    std::vector<char>                              _readBuf;
    EasyBuffer                                     _writeBuf;
    Value                                          _val;
    Value                                          _nullVal;
    std::vector <TypeEnum>                         _inputTypes;
    std::vector <std::string>                      _inputNames;
    int32_t                                        _rNanInt32;
    double                                         _rNanDouble;

    void writeDF(std::vector<ConstChunk const*> const& chunks, int32_t const numRows, ChildProcess& child);
    void writeFinalDF(ChildProcess& child);
    void readDF(ChildProcess& child, bool lastMessage = false);
};


}}
#endif /* SRC_DFINTERFACE_H_ */
