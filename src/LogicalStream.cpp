/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2020 Paradigm4 Inc.
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
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <fstream>
#include "StreamSettings.h"
#include "TSVInterface.h"
#include "DFInterface.h"
#include "FeatherInterface.h"
#include <rbac/Rbac.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>

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
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(RE::PLUS, {
                    RE(PP(PLACEHOLDER_INPUT))
                 }),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
              })
            },
            { KW_FORMAT, RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_CHUNK_SIZE, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { KW_TYPES, RE(RE::OR, {
                           RE(PP(PLACEHOLDER_EXPRESSION, TID_STRING)),
                           RE(RE::GROUP, {
                                  RE(PP(PLACEHOLDER_EXPRESSION, TID_STRING)),
                                  RE(RE::PLUS, {
                                     RE(PP(PLACEHOLDER_EXPRESSION, TID_STRING))
                              })
                           })
                        })
            },
            { KW_NAMES, RE(RE::OR, {
                           RE(PP(PLACEHOLDER_EXPRESSION, TID_STRING)),
                           RE(RE::GROUP, {
                                  RE(PP(PLACEHOLDER_EXPRESSION, TID_STRING)),
                                  RE(RE::PLUS, {
                                     RE(PP(PLACEHOLDER_EXPRESSION, TID_STRING))
                              })
                           })
                        })
            }
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query)
    {
        //Read the file at /opt/scidb/VV.VV/etc/stream_allowed, one command per line
        //If our command is in that file, it is "blessed" and we let it run by anyone.
        //Otherwise, the user needs to be in the 'operator' role.
        uint32_t major = SCIDB_VERSION_MAJOR();
        uint32_t minor = SCIDB_VERSION_MINOR();
        std::ostringstream commandsFile;
        commandsFile<<"/opt/scidb/"<<major<<"."<<minor<<"/etc/stream_allowed";
        Settings settings(_parameters, _kwParameters, true, query);
        std::string const& command = settings.getCommand();
    	std::ifstream infile(commandsFile.str());
        std::string line;
        while (std::getline(infile, line))
        {
            if(line == command)
            {
                return;
            }
        }
        query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, shared_ptr<Query> query)
    {
        if(schemas.size() > 2)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "can't support more than two input arrays";
        }
        Settings settings(_parameters, _kwParameters, true, query);
        if(settings.getFormat() == TSV)
        {
            return TSVInterface::getOutputSchema(schemas, settings, query);
        }
        else if(settings.getFormat() == DF)
        {
            return DFInterface::getOutputSchema(schemas, settings, query);
        }
        else
        {
            return FeatherInterface::getOutputSchema(schemas, settings, query);
        }
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalStream, "stream");

} // emd namespace scidb
