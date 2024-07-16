#include "ConnectionDB.h"
#include <cctype>
#include <vector>

#include <libpq-fe.h>
#include <sqlite3.h>

static const int MAX_POOL_COUNT = 1024;

static std::pair<std::string, int> replaceParameters(std::string_view prepare)
{
    std::string str;
    int count = 1;

    for(auto begin = prepare.begin(), end = prepare.end(); begin < end; begin++)
    {
        if(*begin == '?')
        {
           str.push_back('$');
           str += std::to_string(count);
           count++;
        }
        else if(*begin == '\'')
        {
           str.push_back(*begin);
           begin++;

           while(begin < end)
           {
             if(*begin == '\'')
             {
                str.push_back(*begin);
                begin++;

                break;
             }

             str.push_back(*begin);
             begin++;
           }

           begin--;
        }
        else if(*begin == '"')
        {
           str.push_back(*begin);
           begin++;

           while(begin < end)
           {
             if(*begin == '"')
             {
                str.push_back(*begin);
                begin++;

                break;
             }

             str.push_back(*begin);
             begin++;
           }

           begin--;
        }
        else str.push_back(*begin);
    }

    count--;

    return {str, count};
}

static bool strlcmp(const char * input, const char * lower)
{
    while(*input != '\0')
    {
          if(*lower == '\0') return true;
          if(std::tolower(*input) != *lower) return false;

          input++;
          lower++;
    }

    return (*lower == '\0');
}

//===================================================================

std::string ConnectionDB::sqlEscaping(const std::string & value)
{
    std::string ret;
    ret.reserve((value.size() * 2) + 2);

    ret.push_back('\'');

    for(char v : value)
    {
        if(v == '\'') ret.append("''");
        else ret.push_back(v);
    }

    ret.push_back('\'');

    ret.shrink_to_fit();
    return ret;
}

//===================================================================

void ConnectionPostgreSQL::clearResurce()
{
    next_pos = 0;

    if(res != nullptr)
    {
       PQclear(res);
       res = nullptr;
    }

    if(stmtName.size() > 0)
    {
       boundCount = 0;
       bound.clear();
       PQexec(conn, ("DEALLOCATE " + stmtName).data());
       stmtName.clear();
    }
}

bool ConnectionPostgreSQL::firstSingleRow()
{
    res = PQgetResult(conn);

    if(res == nullptr) return true;

    switch(PQresultStatus(res))
    {
       case PGRES_SINGLE_TUPLE: return true;

       //single row mode: insert, update, create, ...

       case PGRES_COMMAND_OK:
       {
            PQclear(res);
            res = nullptr;
            return true;
       }

       //--------------------------------------------

       case PGRES_TUPLES_OK:
       {
            do{ PQclear(res); }while((res = PQgetResult(conn)) != nullptr);
            return true;
       }
       default:
       {
            setError(PQerrorMessage(conn));
            do{ PQclear(res); }while((res = PQgetResult(conn)) != nullptr);
            return false;
       }
    }
}

ConnectionPostgreSQL::ConnectionPostgreSQL(const std::function<void (std::string_view)> & logger, bool singleRow):ConnectionDB("PostgreSQL", logger), singleRow(singleRow){}

ConnectionPostgreSQL::~ConnectionPostgreSQL()
{
    close();
}

bool ConnectionPostgreSQL::open(std::string_view connectionInfo)
{
    if(conn != nullptr) return false;

    conn = PQconnectdb(connectionInfo.data());

    if(PQstatus(conn) == CONNECTION_OK) return true;

    setError(PQerrorMessage(conn));
    PQfinish(conn);
    conn = nullptr;

    return false;
}

bool ConnectionPostgreSQL::isOpen() const
{
    if(conn == nullptr) return false;
    if(PQstatus(conn) == CONNECTION_OK) return true;

    PQreset(conn);

    if(PQstatus(conn) == CONNECTION_OK) return true;
    return false;
}

void ConnectionPostgreSQL::close()
{
    if(conn == nullptr) return;

    clearResurce();

    PQfinish(conn);
    conn = nullptr;
}

bool ConnectionPostgreSQL::execute(std::string_view query)
{
    if(conn == nullptr) return false;

    clearResurce();

    if(singleRow)
    {
       if(!prepare(query)) return false;
       return exec();
    }

    res = PQexec(conn, query.data());

    switch(PQresultStatus(res))
    {
           case PGRES_COMMAND_OK:
           {
                PQclear(res);
                res = nullptr;

                return true;
           }
           case PGRES_TUPLES_OK:
           {
                return true;
           }

           default:
           {
                setError(PQerrorMessage(conn));

                PQclear(res);
                res = nullptr;

                return false;
           }
    }
}

bool ConnectionPostgreSQL::prepare(std::string_view prepare)
{
    if(conn == nullptr) return false;

    clearResurce();

    stmtCounter++;
    stmtName = "stmt_" + std::to_string(stmtCounter);

    auto pair = replaceParameters(prepare);
    boundCount = pair.second;

    PGresult * stmt;

    if(singleRow)
    {
       if(!PQsendPrepare(conn, stmtName.data(), pair.first.data(), pair.second, nullptr))
       {
          setError(PQerrorMessage(conn));
          return false;
       }

       stmt = PQgetResult(conn);

       while((res = PQgetResult(conn)) != nullptr) PQclear(res);
    }
    else stmt = PQprepare(conn, stmtName.data(), pair.first.data(), pair.second, nullptr);

    if(PQresultStatus(stmt) != PGRES_COMMAND_OK)
    {
       setError(PQerrorMessage(conn));
       PQclear(stmt);

       return false;
    }

    PQclear(stmt);
    return true;
}

void ConnectionPostgreSQL::bind(int pos, std::string_view value)
{
    if(pos < 0 || pos >= boundCount) return;
    bound[pos] = value;
}

bool ConnectionPostgreSQL::exec()
{
    if(stmtName.size() == 0) return false;

    next_pos = 0;
    PQclear(res);
    res = nullptr;

    std::vector<char *> values;
    std::vector<int> lengths, formats(bound.size(), 0);

    for(auto & v : bound)
    {
        values.push_back(v.second.data());
        lengths.push_back(v.second.size());
    }

    if(singleRow)
    {
       if(!PQsendQueryPrepared(conn, stmtName.data(), bound.size(), values.data(), lengths.data(), formats.data(), 0))
       {
          setError(PQerrorMessage(conn));
          return false;
       }

       if((isSingleRow = PQsetSingleRowMode(conn))) return firstSingleRow();

       return true;
    }

    res = PQexecPrepared(conn, stmtName.data(), bound.size(), values.data(), lengths.data(), formats.data(), 0);

    switch(PQresultStatus(res))
    {
           case PGRES_COMMAND_OK:
           {
                PQclear(res);
                res = nullptr;

                return true;
           }
           case PGRES_TUPLES_OK:
           {
                return true;
           }

           default:
           {
                setError(PQerrorMessage(conn));

                PQclear(res);
                res = nullptr;

                return false;
           }
    }
}

int ConnectionPostgreSQL::fieldCount()
{
    return (res == nullptr) ? 0 : PQnfields(res);
}

std::string ConnectionPostgreSQL::fieldName(int fieldIndex)
{
    return (res == nullptr || fieldIndex < 0 || PQnfields(res) <= fieldIndex) ? 0 : PQfname(res, fieldIndex);
}

ConnectionDB::FieldType ConnectionPostgreSQL::fieldType(int fieldIndex)
{
    if (res == nullptr || fieldIndex < 0 || PQnfields(res) <= fieldIndex) return FieldType::None;

    switch(PQftype(res, fieldIndex))
    {
        case 4: return FieldType::Null;
        case 16: return FieldType::Bool;
        case 18:
        case 25:
        case 1042:
        case 1043: return FieldType::String;
        case 20:
        case 21:
        case 23: return FieldType::Int;
        case 700:
        case 701:
        case 790: return FieldType::Double;
        case 1700: return FieldType::Numeric;
        case 1082: return FieldType::Date;
        case 1083: return FieldType::Time;
        case 1266: return FieldType::TimeWithTimeZone;
        case 1114:
        case 13413: return FieldType::DateTime;
        case 1184: return FieldType::DateTimeWithTimeZone;
        case 17: return FieldType::Blob;
        case 2950: return FieldType::Uuid;
        case 114: return FieldType::Json;
        case 142: return FieldType::Xml;
        default: return FieldType::Unknown;
    }
}

bool ConnectionPostgreSQL::next()
{
    if(singleRow && isSingleRow)
    {
       if(conn == nullptr) return false;

       if(next_pos == 0 && res != nullptr)
       {
          next_pos++;
          return true;
       }

       if(res != nullptr) PQclear(res);

       res = PQgetResult(conn);
       if(res == nullptr) return false;

       switch(PQresultStatus(res))
       {
          case PGRES_SINGLE_TUPLE: return true;
          case PGRES_TUPLES_OK:
          {
               do{ PQclear(res); }while((res = PQgetResult(conn)) != nullptr);
               return false;
          }
          default:
          {
               setError(PQerrorMessage(conn));
               do{ PQclear(res); }while((res = PQgetResult(conn)) != nullptr);
               return false;
          }
       }
    }

    if(res == nullptr || next_pos == PQntuples(res)) return false;

    next_pos++;

    return true;
}

std::string ConnectionPostgreSQL::value(int fieldIndex)
{
    return (res == nullptr || fieldIndex < 0 || fieldIndex >= PQnfields(res))
            ? std::string() : PQgetvalue(res, (singleRow && isSingleRow) ? 0 : next_pos - 1, fieldIndex);
}

static const char * const pg_tables = "select cl.relname from pg_namespace pgn join pg_class cl on cl.relnamespace = pgn.oid and cl.relkind = any(array['r'::\"char\", 'p'::\"char\"]) where pgn.nspname = 'public'";

std::set<std::string> ConnectionPostgreSQL::tables()
{
    std::set<std::string> tables;
    if(!execute(pg_tables)) return tables;
    while(next()) tables.insert(value(0));
    clearResurce();
    return tables;
}

//=====================================================================================

void ConnectionSqlite::clearResurce()
{
    if(stmt != nullptr)
    {
       bound.clear();
       sqlite3_finalize(stmt);
       stmt = nullptr;
    }
}

bool ConnectionSqlite::prepare_stmt(std::string_view query, bool prepare)
{
    clearResurce();

    if(sqlite3_prepare_v3(db, query.data(), -1, 0, &stmt, nullptr) == SQLITE_OK)
    {
       if(prepare)
       {
          return true;
       }
       else if(sqlite3_bind_parameter_count(stmt) == 0)
       {
          switch(sqlite3_step(stmt))
          {
                 case SQLITE_ROW:
                 {
                      isFirst = true;
                      return true;
                 }

                 case SQLITE_DONE:
                 {
                      sqlite3_finalize(stmt);
                      stmt = nullptr;
                      return true;
                 }

                 default:
                 {
                      setError(sqlite3_errmsg(db));
                      sqlite3_finalize(stmt);
                      stmt = nullptr;
                      return false;
                 }
           }

           return true;
       }

       setError("method 'execute' does not support bound values");

    }else setError(sqlite3_errmsg(db));

    sqlite3_finalize(stmt);
    stmt = nullptr;

    return false;
}

ConnectionSqlite::ConnectionSqlite(const std::function<void (std::string_view)> & logger):ConnectionDB("SQLite", logger){}

ConnectionSqlite::~ConnectionSqlite()
{
    close();
}

bool ConnectionSqlite::open(std::string_view connectionInfo)
{
    if(db != nullptr) return false;

    if(sqlite3_open_v2(connectionInfo.data(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) == SQLITE_OK) return true;

    setError(sqlite3_errmsg(db));
    sqlite3_close_v2(db);
    db = nullptr;

    return false;
}

bool ConnectionSqlite::isOpen() const
{
    return (db != nullptr);
}

void ConnectionSqlite::close()
{
    if(db == nullptr) return;

    clearResurce();

    sqlite3_close_v2(db);
    db = nullptr;
}

bool ConnectionSqlite::execute(std::string_view query)
{  
    if(db == nullptr) return false;

    isPrepare = false;

    return prepare_stmt(query, false);
}

bool ConnectionSqlite::prepare(std::string_view prepare)
{
    if(db == nullptr) return false;

    int ret = prepare_stmt(prepare, true);

    isPrepare = true;
    isExec = false;

    return ret;
}

void ConnectionSqlite::bind(int pos, std::string_view value)
{
    if(stmt == nullptr || !isPrepare || pos < 0 || pos >= sqlite3_bind_parameter_count(stmt)) return;
    bound[++pos] = value;
}

bool ConnectionSqlite::exec()
{
    if(stmt == nullptr || !isPrepare) return false;

    if(isExec) sqlite3_reset(stmt);
    else isExec = true;

    isFirst = false;

    for(const auto & p : bound) sqlite3_bind_text(stmt, p.first, p.second.data(), -1, nullptr);

    switch(sqlite3_step(stmt))
    {
           case SQLITE_ROW:
           {
                isFirst = true;
                return true;
           }

           case SQLITE_DONE: return true;

           default:
           {
                setError(sqlite3_errmsg(db));
                return false;
           }
    }
}

int ConnectionSqlite::fieldCount()
{
    return (stmt == nullptr) ? 0 : sqlite3_column_count(stmt);
}

std::string ConnectionSqlite::fieldName(int fieldIndex)
{
    return (stmt == nullptr || fieldIndex < 0 || fieldIndex >= sqlite3_column_count(stmt)) ? std::string() : sqlite3_column_name(stmt, fieldIndex);
}

ConnectionDB::FieldType ConnectionSqlite::fieldType(int fieldIndex)
{
    if(stmt == nullptr || fieldIndex < 0 || fieldIndex >= sqlite3_column_count(stmt)) return FieldType::None;

    const char * type = sqlite3_column_decltype(stmt, fieldIndex);

    if(type == nullptr) return FieldType::None;

    if(strlcmp(type,"boolean")
       || strlcmp(type,"bool")) return FieldType::Bool;

    if(strlcmp(type,"integer")
       || strlcmp(type,"int")
       || strlcmp(type,"tinyint")
       || strlcmp(type,"smallint")
       || strlcmp(type,"mediumint")
       || strlcmp(type,"bigint")
       || strlcmp(type,"unsigned big int")) return FieldType::Int;

    if(strlcmp(type,"double")
       || strlcmp(type,"float")
       || strlcmp(type,"real")
       || strlcmp(type,"numeric")
       || strlcmp(type,"decimal")
       || strlcmp(type,"double precision")) return FieldType::Double;

    if(strlcmp(type,"text")
       || strlcmp(type,"char")
       || strlcmp(type,"varchar")
       || strlcmp(type,"character")
       || strlcmp(type,"varying character")
       || strlcmp(type,"nchar")
       || strlcmp(type,"native character")
       || strlcmp(type,"nvarchar")
       || strlcmp(type,"clob")) return FieldType::String;

    if(strlcmp(type,"datetime")
       || strlcmp(type,"timestamp")) return FieldType::DateTime;

    if(strlcmp(type,"date")) return FieldType::Date;

    if(strlcmp(type,"time")) return FieldType::Time;

    if(strlcmp(type,"blob")
       || strlcmp(type,"memo")) return FieldType::Blob;

    return FieldType::Unknown;
}

bool ConnectionSqlite::next()
{
    if(stmt == nullptr) return false;

    if(isPrepare && !isExec) return false;

    if(isFirst)
    {
       isFirst = false;
       return true;
    }

    return (sqlite3_step(stmt) == SQLITE_ROW);
}

std::string ConnectionSqlite::value(int fieldIndex)
{
    return (stmt == nullptr || fieldIndex < 0 || fieldIndex >= sqlite3_column_count(stmt))
            ? std::string() : (sqlite3_column_type(stmt, fieldIndex) == 5) ? "" : reinterpret_cast<const char *>(sqlite3_column_text(stmt, fieldIndex));
}

static const char * const sqlite_tables = "select lower(name) from sqlite_schema where type = 'table' and name not like 'sqlite_%'";

std::set<std::string> ConnectionSqlite::tables()
{
    std::set<std::string> tables;
    if(!execute(sqlite_tables)) return tables;
    while(next()) tables.insert(value(0));
    clearResurce();
    return tables;
}

//==================================================================================================

class PoolPointer
{
      ConnectionDBPool * pool;

 public:
      explicit PoolPointer() = delete;
      explicit PoolPointer(ConnectionDBPool * pool);
      void freeConnection(std::shared_ptr<ConnectionDB> && connection);
};

PoolPointer::PoolPointer(ConnectionDBPool * pool):pool(pool){}

void PoolPointer::freeConnection(std::shared_ptr<ConnectionDB> && connection)
{
     pool->freeConnection(std::move(connection));
}

//---------------------------------------------------------------------------------------------------

TempConnectionDB::TempConnectionDB(){}

TempConnectionDB::TempConnectionDB(std::shared_ptr<ConnectionDB> && conn, const std::shared_ptr<PoolPointer> & pointer):conn(std::move(conn)), pointer(pointer){}

TempConnectionDB::~TempConnectionDB()
{
    returnToPoolDB();
}

bool TempConnectionDB::isValid() const
{
    return (conn) ? true : false;
}

void TempConnectionDB::returnToPoolDB()
{
    if(!conn || pointer.expired()) return;

    std::shared_ptr<PoolPointer> p = pointer.lock();

    if(p) p->freeConnection(std::move(conn));
    else conn = nullptr;
}

std::string TempConnectionDB::error() const
{
    return (conn) ? conn->error() : std::string();
}

bool TempConnectionDB::isOpen() const
{
    return (conn) ? conn->isOpen() : false;
}

bool TempConnectionDB::execute(std::string_view query)
{
    return (conn) ? conn->execute(query) : false;
}

bool TempConnectionDB::prepare(std::string_view prepare)
{
    return (conn) ? conn->prepare(prepare) : false;
}

void TempConnectionDB::bind(int pos, std::string_view value)
{
    if(conn) conn->bind(pos, value);
}

bool TempConnectionDB::exec()
{
    return (conn) ? conn->exec() : false;
}

int TempConnectionDB::fieldCount()
{
    return (conn) ? conn->fieldCount() : false;
}

std::string TempConnectionDB::fieldName(int fieldIndex)
{
    return (conn) ? conn->fieldName(fieldIndex) : std::string();
}

ConnectionDB::FieldType TempConnectionDB::fieldType(int fieldIndex)
{
    return (conn) ? conn->fieldType(fieldIndex) : ConnectionDB::FieldType::None;
}

bool TempConnectionDB::next()
{
    return (conn) ? conn->next() : false;
}

std::string TempConnectionDB::value(int fieldIndex)
{
    return (conn) ? conn->value(fieldIndex) : std::string();
}

std::set<std::string> TempConnectionDB::tables()
{
    return (conn) ? conn->tables() : std::set<std::string>();
}

//---------------------------------------------------------------------------------------------------

ConnectionDBPool::ConnectionDBPool():pointer(std::make_shared<PoolPointer>(this)){}

bool ConnectionDBPool::createPool(ConnectionType type, int poolCount, std::string_view connectionInfo, const std::function<void (std::string_view)> & logger)
{
    if(poolCount < 0 && poolCount > MAX_POOL_COUNT) return false;

    for(int i = 0; i < poolCount; i++)
    {
        std::shared_ptr<ConnectionDB> conn;

        if(type == PostgreSQL) conn = std::make_shared<ConnectionPostgreSQL>(logger);
        else conn = std::make_shared<ConnectionSqlite>(logger);

        if(!conn->open(connectionInfo)) return false;

        connections.emplace(std::move(conn));
    }

    return true;
}

TempConnectionDB ConnectionDBPool::connection()
{
    std::unique_lock<std::mutex> lock(c_mutex);

    while(connections.empty()) condition.wait(lock);

    std::shared_ptr<ConnectionDB> conn = connections.front();

    connections.pop();

    return TempConnectionDB(std::move(conn), pointer);
}

void ConnectionDBPool::freeConnection(std::shared_ptr<ConnectionDB> && connection)
{
    std::unique_lock<std::mutex> lock(c_mutex);

    connections.push(std::move(connection));

    lock.unlock();
    condition.notify_one();
}

//---------------------------------------------------------------------------------------------------

std::mutex ConnectionDBPool::p_mutex = std::mutex();
std::map<std::string, std::shared_ptr<ConnectionDBPool>> ConnectionDBPool::pools = std::map<std::string, std::shared_ptr<ConnectionDBPool>>();

bool ConnectionDBPool::open(std::string_view connectionName, ConnectionType type, int poolCount, std::string_view connectionInfo, const std::function<void (std::string_view)> & logger)
{
    std::lock_guard<std::mutex> lock(p_mutex);
    std::string key(connectionName);
    if(pools.contains(key)) return false;

    std::shared_ptr<ConnectionDBPool>pool = std::make_shared<ConnectionDBPool>();
    if(!pool->createPool(type, poolCount, connectionInfo, logger)) return false;

    pools[key] = pool;
    return true;
}

bool ConnectionDBPool::isOpen(std::string_view connectionName)
{
    std::lock_guard<std::mutex> lock(p_mutex);
    return pools.contains(std::string(connectionName));
}

TempConnectionDB ConnectionDBPool::connection(std::string_view connectionName)
{
    std::shared_ptr<ConnectionDBPool> pool;

    {
       std::lock_guard<std::mutex> lock(p_mutex);
       pool = pools[std::string(connectionName)];
    }

    if(!pool) return TempConnectionDB();

    return pool->connection();
}

void ConnectionDBPool::close(std::string_view connectionName)
{
    std::lock_guard<std::mutex> lock(p_mutex);
    pools.erase(std::string(connectionName));
}

