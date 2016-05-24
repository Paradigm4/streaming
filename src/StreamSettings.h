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

#ifndef SRC_STREAMSETTINGS_H_
#define SRC_STREAMSETTINGS_H_

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include <log4cxx/logger.h>

using boost::algorithm::trim;
using boost::algorithm::trim_left_if;
using boost::algorithm::trim_right_if;
using boost::algorithm::split;
using boost::algorithm::is_any_of;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using std::vector;
using std::shared_ptr;
using std::string;
using std::ostringstream;

namespace scidb { namespace stream
{

enum DFDataType
{
    STRING  = 0,
    DOUBLE  = 1,
    INTEGER = 2
};

enum TransferFormat
{
    TSV,   //text tsv
    DF     //R data.frame
};

class Settings
{
private:
    TransferFormat      _transferFormat;
    vector<DFDataType>  _dfTypes;
    ssize_t             _outputChunkSize;
    string              _command;

public:
    static const size_t MAX_PARAMETERS = 4;

    Settings(vector<shared_ptr <OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
                 _transferFormat(TSV),
                 _dfTypes(0),
                 _outputChunkSize(10000000)
     {
        string const formatHeader                  = "format=";
        string const chunkSizeHeader               = "chunk_size=";
        string const typesHeader                   = "types=";
        bool formatSet    = false;
        bool typesSet     = false;
        bool chunkSizeSet = false;
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to stream";
        }
        if(logical)
        {
            _command = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) operatorParameters[0])->getExpression(),query, TID_STRING).getString();
        }
        else
        {
            _command = ((shared_ptr<OperatorParamPhysicalExpression>&) operatorParameters[0])->getExpression()->evaluate().getString();
        }
        for (size_t i= 1; i<nParams; ++i)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            string parameterString;
            if (logical)
            {
                parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
            }
            else
            {
                parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
            if (starts_with(parameterString, chunkSizeHeader))
            {
                if (chunkSizeSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set chunk_size multiple times";
                }
                string paramContent = parameterString.substr(chunkSizeHeader.size());
                trim(paramContent);
                try
                {
                    _outputChunkSize = lexical_cast<int64_t>(paramContent);
                    if(_outputChunkSize <= 0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk_size must be positive";
                    }
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse chunk_size";
                }
            }
            else if (starts_with(parameterString, formatHeader))
            {
                if (formatSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set format multiple times";
                }
                string paramContent = parameterString.substr(formatHeader.size());
                trim(paramContent);
                if(paramContent == "tsv")
                {
                    _transferFormat = TSV;
                }
                else if(paramContent == "df")
                {
                    _transferFormat = DF;
                }
                else
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse format";
                }
            }
            else if (starts_with(parameterString, typesHeader))
            {
                if(typesSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set types multiple times";
                }
                string paramContent = parameterString.substr(typesHeader.size());
                trim(paramContent);
                trim_left_if(paramContent, is_any_of("("));
                trim_right_if(paramContent, is_any_of(")"));
                vector<string> tokens;
                split(tokens, paramContent, is_any_of(","));
                if(tokens.size() == 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse types";
                }
                for(size_t i =0; i<tokens.size(); ++i)
                {
                    string const& t = tokens[i];
                    if(t == "int32")
                    {
                        _dfTypes.push_back(INTEGER);
                    }
                    else if(t == "double")
                    {
                        _dfTypes.push_back(DOUBLE);
                    }
                    else if(t == "string")
                    {
                        _dfTypes.push_back(STRING);
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "strings not supported yet";
                    }
                    else
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse types";
                    }
                }
            }
            else
            {
                ostringstream error;
                error << "Unrecognized token '"<<parameterString<<"'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        if(_transferFormat == DF && _dfTypes.size() == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "when using the df format, types= must be specified";
        }
    }

    TransferFormat getFormat() const
    {
        return _transferFormat;
    }

    vector<DFDataType> const& getTypes() const
    {
        return _dfTypes;
    }

    size_t getChunkSize() const
    {
        return _outputChunkSize;
    }

    string const& getCommand() const
    {
        return _command;
    }

};

} }

#endif /* SRC_STREAMSETTINGS_H_ */
