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

#include "StreamSettings.h"
#include "ChildProcess.h"
#include "../lib/slave.h"
#include "../lib/serial.h"
#include "TSVInterface.h"
#include "DFInterface.h"

using std::shared_ptr;
using std::make_shared;
using std::string;
using std::ostringstream;
using std::vector;

namespace scidb { namespace stream
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.stream"));


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

    template <typename INTERFACE>
    shared_ptr<Array> runStream(vector <shared_ptr<Array> > &inputArrays, Settings const& settings, shared_ptr<Query>& query)
    {
        ChildProcess child(settings.getCommand(), query);
        INTERFACE interface(settings, _schema, query);
        if(inputArrays.size() == 2)
        {
            shared_ptr<Array> preArray = inputArrays[1];
            ArrayDesc const& preSchema = preArray->getArrayDesc();
            interface.setInputSchema(preSchema);
            size_t const nAttrs = preSchema.getAttributes(true).size();
            vector <shared_ptr<ConstArrayIterator> > aiters (nAttrs);
            vector<ConstChunk const*> chunks(nAttrs, NULL);
            for(size_t i =0; i<nAttrs; ++i)
            {
                aiters[i] = preArray->getConstIterator(i);
            }
            while(!aiters[0]->end())
            {
                for(size_t i =0; i<nAttrs; ++i)
                {
                   chunks[i]= &(aiters[i]->getChunk());
                }
                interface.streamData(chunks, child);
                for(size_t i =0; i<nAttrs; ++i)
                {
                    ++(*aiters[i]);
                }
            }
        }
        shared_ptr<Array> inputArray = inputArrays[0];
        ArrayDesc const& inputSchema = inputArray ->getArrayDesc();
        interface.setInputSchema(inputSchema);
        size_t const nAttrs = inputSchema.getAttributes(true).size();
        vector <shared_ptr<ConstArrayIterator> > aiters (nAttrs);
        vector<ConstChunk const*> chunks(nAttrs, NULL);
        for(size_t i =0; i<nAttrs; ++i)
        {
            aiters[i] = inputArray->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            for(size_t i =0; i<nAttrs; ++i)
            {
               chunks[i]= &(aiters[i]->getChunk());
            }
            interface.streamData(chunks, child);
            for(size_t i =0; i<nAttrs; ++i)
            {
                ++(*aiters[i]);
            }
        }
        return interface.finalize(child);
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(
               std::vector<RedistributeContext> const& inputDistributions,
               std::vector< ArrayDesc> const& inputSchemas) const
    {
        return RedistributeContext(createDistribution(psUndefined), _schema.getResidency() );
    }

    shared_ptr< Array> execute(std::vector< shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        Settings settings(_parameters, false, query);
        if(settings.getFormat() == TSV)
        {
            return runStream<TSVInterface>(inputArrays, settings, query);
        }
        else
        {
            return runStream<DFInterface> (inputArrays, settings, query);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalStream, "stream", "PhysicalStream");

} // end namespace scidb
