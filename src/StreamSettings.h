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
#include <query/PhysicalOperator.h>
#include <query/OperatorParam.h>
#include <query/Query.h>
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

namespace scidb {
namespace stream
{
// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.stream"));

static const char* const KW_FORMAT = "format";
static const char* const KW_CHUNK_SIZE = "chunk_size";
static const char* const KW_TYPES = "types";
static const char* const KW_NAMES = "names";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

    enum TransferFormat
{
    TSV,     // text tsv
    DF,      // R data.frame
    FEATHER  // Apache Arrow Feather format
};

class Settings
{
private:
    TransferFormat      _transferFormat;
    vector<TypeEnum>    _types;
    vector<string>      _names;
    ssize_t             _outputChunkSize;
    bool				_chunkSizeSet;
    string              _command;

public:
    static const size_t MAX_PARAMETERS = 5;

private:
    void setParamDfNames(vector<string> names)
    {
        LOG4CXX_DEBUG(logger, "stream dataframe out name size " << names.size());
        for (size_t i = 0; i < names.size(); ++i) {
            _names.push_back(names[i]);
            LOG4CXX_DEBUG(logger, "stream dataframe out name " << i << " is " << names[i]);
        }
    }

    void setParamDfTypes(vector<string> tokens)
    {
        for(size_t i =0; i<tokens.size(); ++i)
        {
            string const& t = tokens[i];
            if(t == "int32")
            {
                _types.push_back(TE_INT32);
            }
            else if(t == "int64")
            {
                _types.push_back(TE_INT64);
            }
            else if(t == "double")
            {
                _types.push_back(TE_DOUBLE);
            }
            else if(t == "string")
            {
                _types.push_back(TE_STRING);
            }
            else if(t == "binary")
            {
                _types.push_back(TE_BINARY);
            }
            else
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse types";
            }
            LOG4CXX_DEBUG(logger, "stream dataframe out type " << i << " is " << t);
        }
    }

    void checkIfSet(bool alreadySet, const char* kw)
    {
        if (alreadySet)
        {
            ostringstream error;
            error<<"illegal attempt to set "<<kw<<" multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
    }

    void setParamChunkSize(vector<int64_t> keys)
    {
        int64_t res = keys[0];
        if(res <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk size must be positive";
        }
        _outputChunkSize = res;
    }

    void setParamFormat(vector<string> keys)
    {
        string trimmedContent = keys[0];
        if(trimmedContent == "tsv")
        {
            _transferFormat = TSV;
        }
        else if(trimmedContent == "df")
        {
            _transferFormat = DF;
        }
        else if(trimmedContent == "feather")
        {
            _transferFormat = FEATHER;
        }
        else
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse format";
        }
    }

    void setKeywordParamString(KeywordParameters const& kwParams, const char* const kw, bool& alreadySet, void (Settings::* innersetter)(vector<string>) )
    {
        checkIfSet(alreadySet, kw);
        vector <string> paramContent;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                for (size_t i = 0; i < gParams.size(); ++i) {
                    paramContent.push_back(getParamContentString(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentString(kwParam));
            }
            (this->*innersetter)(paramContent);
            alreadySet = true;
        } else {
            LOG4CXX_DEBUG(logger, "stream findKeyword null: " << kw);
        }
    }

    void setKeywordParamInt64(KeywordParameters const& kwParams, const char* const kw, bool& alreadySet, void (Settings::* innersetter)(vector<int64_t>) )
    {
        checkIfSet(alreadySet, kw);

        vector<int64_t> paramContent;
        size_t numParams;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                numParams = gParams.size();
                for (size_t i = 0; i < numParams; ++i) {
                    paramContent.push_back(getParamContentInt64(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentInt64(kwParam));
            }
            (this->*innersetter)(paramContent);
            alreadySet = true;
        } else {
            LOG4CXX_DEBUG(logger, "Stream findKeyword null: " << kw);
        }
    }

    string getParamContentString(Parameter& param)
    {
        string paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_STRING).getString();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getString();
        }
        return paramContent;
    }

    int64_t getParamContentInt64(Parameter& param)
    {
        size_t paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_INT64).getInt64();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getInt64();
            LOG4CXX_DEBUG(logger, "Stream integer param is " << paramContent)

        }
        return paramContent;
    }

    Parameter getKeywordParam(KeywordParameters const& kwp, const std::string& kw) const
    {
        auto const& kwPair = kwp.find(kw);
        return kwPair == kwp.end() ? Parameter() : kwPair->second;
    }

    string paramToString(shared_ptr <OperatorParam> const& parameter, shared_ptr<Query>& query, bool logical)
    {
        if(logical)
        {
            string result = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) parameter)->getExpression(), TID_STRING).getString();
            return result;
        }
        return ((shared_ptr<OperatorParamPhysicalExpression>&) parameter)->getExpression()->evaluate().getString();
    }

public:
    Settings(vector<shared_ptr <OperatorParam> > const& operatorParameters,
             KeywordParameters const& kwParams,
             bool logical,
             shared_ptr<Query>& query):
                 _transferFormat(TSV),
                 _types(0),
                 _outputChunkSize(1024*1024*1024),
                 _chunkSizeSet(false)
     {
        bool formatSet    = false;
        bool typesSet     = false;
        bool namesSet     = false;
        size_t const nParams = operatorParameters.size();

        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to stream";
        }
        _command = paramToString(operatorParameters[0], query, logical);
        LOG4CXX_DEBUG(logger, "Stream command is " << _command)

        setKeywordParamInt64(kwParams, KW_CHUNK_SIZE, _chunkSizeSet, &Settings::setParamChunkSize);
        setKeywordParamString(kwParams, KW_FORMAT, formatSet, &Settings::setParamFormat);
        setKeywordParamString(kwParams, KW_TYPES, typesSet, &Settings::setParamDfTypes);
        setKeywordParamString(kwParams, KW_NAMES, namesSet, &Settings::setParamDfNames);

    }

    TransferFormat getFormat() const
    {
        return _transferFormat;
    }

    vector<TypeEnum> const& getTypes() const
    {
        return _types;
    }

    vector<string> const& getNames() const
    {
        return _names;
    }

    size_t getChunkSize() const
    {
        return _outputChunkSize;
    }

    bool isChunkSizeSet() const
    {
        return _chunkSizeSet;
    }

    string const& getCommand() const
    {
        return _command;
    }

};

} }

#endif /* SRC_STREAMSETTINGS_H_ */
