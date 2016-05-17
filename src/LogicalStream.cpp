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

using std::shared_ptr;

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

using namespace std;

namespace scidb
{

class LogicalStream: public LogicalOperator
{
public:
    LogicalStream(const std::string& logicalName, const std::string& alias) :
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("string");
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "status",   TID_STRING,    0, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("instance_id", 0,   query->getInstancesCount()-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("chunk_no",    0,   CoordinateBounds::getMax(),   1, 0));
        return ArrayDesc(inputSchema.getName(), outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalStream, "stream");

} // emd namespace scidb
