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

#ifndef SRC_TSVINTERFACE_H_
#define SRC_TSVINTERFACE_H_

#include <query/Operator.h>
#include <query/TypeSystem.h>

namespace scidb { namespace stream
{

class Settings;
class ChildProcess;

/**
 * Interface for streaming data in TSV format. Converts SciDB data to TSV and then communicates with the child process.
 * The data format is the number of TSV lines, followed by the data, for example:
 *
 * 3
 * 1.234    56  alex
 * 7.890    12  bob
 * 3.456    78  ted
 *
 * An empty message contains 0 lines, like this:
 *
 * 0
 *
 * Some nuances are still not solidified: how to output SciDB NULL codes, or whether strings be quoted and tabs inside
 * strings should be escaped. See the Ctor or the Settings class for some defaults. Couldn't easily reuse any existing
 * SciDB components for the TSV conversion so, sadly, implemented our own TSV conversion here. Upside: more flexibility
 * right here. For UDTs we do attempt to locate a UDT->string conversion function.
 */
class TSVInterface
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
    TSVInterface(Settings const& settings, ArrayDesc const& outputSchema, std::shared_ptr<Query> const& query);

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

    static size_t const MAX_RESPONSE_SIZE = 1024*1024*1024;

private:
    char const                     _attDelim;
    char const                     _lineDelim;
    bool const                     _printCoords;
    std::string                    _nanRepresentation;
    std::string                    _nullRepresentation;
    std::shared_ptr<Query>         _query;
    std::shared_ptr<Array>         _result;
    std::shared_ptr<ArrayIterator> _aiter;
    Coordinates                    _outPos;
    std::vector <TypeEnum>         _inputTypes;
    std::vector<FunctionPointer>   _inputConverters;
    Value                          _stringBuf;

    void convertChunks(std::vector< std::shared_ptr<ConstChunkIterator> > citers, size_t &nCells, std::string& output);
    void writeTSV(size_t const nLines, std::string const& inputData, ChildProcess& child);
    void readTSV (std::string& output, ChildProcess& child, bool last = false);
    void addChunkToArray(std::string const& output);
};

}}

#endif /* SRC_TSVINTERFACE_H_ */
