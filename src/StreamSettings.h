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
    vector<string>      _dfNames;
    ssize_t             _outputChunkSize;
    string              _command;

public:
    static const size_t MAX_PARAMETERS = 4;

private:
    string paramToString(shared_ptr <OperatorParam> const& parameter, shared_ptr<Query>& query, bool logical)
    {
        if(logical)
        {
            string result = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) parameter)->getExpression(),query, TID_STRING).getString();
            return result;
        }
        return ((shared_ptr<OperatorParamPhysicalExpression>&) parameter)->getExpression()->evaluate().getString();
    }

    void setParamChunkSize(string trimmedContent)
    {
        try
        {
            _outputChunkSize = lexical_cast<int64_t>(trimmedContent);
            if(_outputChunkSize <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk size must be positive";
            }
        }
        catch (bad_lexical_cast const& exn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse chunk size";
        }
    }

    void setParamFormat(string trimmedContent)
    {
        if(trimmedContent == "tsv")
        {
            _transferFormat = TSV;
        }
        else if(trimmedContent == "df")
        {
            _transferFormat = DF;
        }
        else
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse format";
        }
    }

    void setParamDfTypes(string trimmedContent)
    {
        trim_left_if(trimmedContent, is_any_of("("));
        trim_right_if(trimmedContent, is_any_of(")"));
        vector<string> tokens;
        split(tokens, trimmedContent, is_any_of(","));
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

    void setParamDfNames(string trimmedContent)
    {
        trim_left_if(trimmedContent, is_any_of("("));
        trim_right_if(trimmedContent, is_any_of(")"));
        vector<string> tokens;
        split(tokens, trimmedContent, is_any_of(","));
        if(tokens.size() == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse names";
        }
        for(size_t i =0; i<tokens.size(); ++i)
        {
            string const& t = tokens[i];
            if(t.size()==0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse names";
            }
            for(size_t j=0; j<i; ++j)
            {
                if(t==tokens[j])
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "duplicate names not allowed";
                }
            }
            for(size_t j=0; j<t.size(); ++j)
            {
                char ch = t[j];
                if( !( (j == 0 && ((ch>='a' && ch<='z') || (ch>='A' && ch<='Z') || ch == '_')) ||
                       (j > 0  && ((ch>='a' && ch<='z') || (ch>='A' && ch<='Z') || (ch>='0' && ch <= '9') || ch == '_' ))))
                {
                    ostringstream error;
                    error<<"invalid name '"<<t<<"'";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str();;
                }
            }
            _dfNames.push_back(t);
        }
    }

    void setParam (string const& parameterString, bool& alreadySet, string const& header, void (Settings::* innersetter)(string) )
    {
        string paramContent = parameterString.substr(header.size());
        if (alreadySet)
        {
            string header = parameterString.substr(0, header.size()-1);
            ostringstream error;
            error<<"illegal attempt to set "<<header<<" multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
        trim(paramContent);
        (this->*innersetter)(paramContent); //TODO:.. tried for an hour with a template first. #thisIsWhyWeCantHaveNiceThings
        alreadySet = true;
    }

public:
    Settings(vector<shared_ptr <OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
                 _transferFormat(TSV),
                 _dfTypes(0),
                 _outputChunkSize(1024*1024*1024)
     {
        string const formatHeader                  = "format=";
        string const chunkSizeHeader               = "chunk_size=";
        string const typesHeader                   = "types=";
        string const namesHeader                   = "names=";
        bool formatSet    = false;
        bool typesSet     = false;
        bool chunkSizeSet = false;
        bool namesSet     = false;
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to stream";
        }
        _command = paramToString(operatorParameters[0], query, logical);
        for (size_t i= 1; i<nParams; ++i)
        {
            string parameterString = paramToString(operatorParameters[i], query, logical);
            if (starts_with(parameterString, chunkSizeHeader))
            {
                setParam(parameterString, chunkSizeSet, chunkSizeHeader, &Settings::setParamChunkSize);
            }
            else if (starts_with(parameterString, formatHeader))
            {
                setParam(parameterString, formatSet, formatHeader, &Settings::setParamFormat);
            }
            else if (starts_with(parameterString, typesHeader))
            {
                setParam(parameterString, typesSet, typesHeader, &Settings::setParamDfTypes);
            }
            else if (starts_with(parameterString, namesHeader))
            {
                setParam(parameterString, namesSet, namesHeader, &Settings::setParamDfNames);
            }
            else
            {
                ostringstream error;
                error << "Unrecognized token '"<<parameterString<<"'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        if(_transferFormat == DF)
        {
            if(typesSet == false)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "when using the df format, types= must be specified";
            }
            if(namesSet == false)
            {
                for(size_t i =0; i<_dfTypes.size(); ++i)
                {
                    ostringstream name;
                    name<<"a"<<i;
                    _dfNames.push_back(name.str());
                }
            }
            else if(_dfNames.size() != _dfTypes.size())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "received inconsistent names and types";
            }
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

    vector<string> const& getNames() const
    {
        return _dfNames;
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
