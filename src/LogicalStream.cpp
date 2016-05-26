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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include "StreamSettings.h"

using std::shared_ptr;

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

using namespace std;

namespace scidb
{

using namespace stream;
class LogicalStream: public LogicalOperator
{
public:
    LogicalStream(const std::string& logicalName, const std::string& alias) :
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("string");
        ADD_PARAM_VARIES();
    }

    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < Settings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("instance_id", 0,   query->getInstancesCount()-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("chunk_no",    0,   CoordinateBounds::getMax(),   1, 0));
        Attributes outputAttributes;
        Settings settings(_parameters, true, query);
        if(settings.getFormat() == TSV)
        {
            outputAttributes.push_back( AttributeDesc(0, "response",   TID_STRING,    0, 0));
        }
        else
        {
            vector<string> attNames  = settings.getNames();
            vector<DFDataType> types = settings.getTypes();
            for(AttributeID i =0; i<types.size(); ++i)
            {
                TypeId attType;
                if(types[i]==STRING)
                {
                    attType=TID_STRING;
                }
                else if(types[i] == DOUBLE)
                {
                    attType=TID_DOUBLE;
                }
                else if(types[i] == INTEGER)
                {
                    attType=TID_INT32;
                }
                else
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "something's up";
                }
                outputAttributes.push_back( AttributeDesc(i,  attNames[i],  attType, AttributeDesc::IS_NULLABLE, 0));
            }
            outputDimensions.push_back(DimensionDesc("value_no",    0,   CoordinateBounds::getMax(),   settings.getChunkSize(), 0));
        }
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        return ArrayDesc(inputSchema.getName(), outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalStream, "stream");

} // emd namespace scidb
