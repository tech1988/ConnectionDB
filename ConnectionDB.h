#ifndef CONNECTIONDB_H
#define CONNECTIONDB_H

#include <string>
#include <map>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <memory>
#include <functional>
#include <set>

class ConnectionDB
{
    std::string dbmsName;
    std::function<void(std::string_view)> logger;
    std::string err;

protected:

    void setError(std::string_view error)
    {
         err = dbmsName + ": " + std::string(error);
         if(logger) logger(err);
    }

    virtual void clearResurce() = 0;

public:
    explicit ConnectionDB(std::string_view dbmsName, const std::function<void(std::string_view)> & logger):dbmsName(dbmsName), logger(logger){}
    virtual ~ConnectionDB(){}

    std::string error() const { return  std::move(err); };

    enum FieldType : unsigned char
    {
         None = 0,
         Unknown,
         Null,
         Bool,
         String,
         Int,
         Double,
         Numeric,
         Date,
         Time,
         TimeWithTimeZone,
         DateTime,
         DateTimeWithTimeZone,
         Blob,
         Uuid,
         Json,
         Xml
    };

    virtual bool open(std::string_view connectionInfo) = 0;
    virtual bool isOpen() const = 0;
    virtual void close() = 0;

    virtual bool execute(std::string_view query) = 0;

    virtual bool prepare(std::string_view prepare) = 0;
    virtual void bind(int pos, std::string_view value) = 0;
    virtual bool exec() = 0;

    virtual int fieldCount() = 0;
    virtual std::string fieldName(int fieldIndex) = 0;
    virtual FieldType fieldType(int fieldIndex) = 0;

    virtual bool next() = 0;
    virtual std::string value(int fieldIndex) = 0;

    virtual std::set<std::string> tables() = 0;

    static std::string sqlEscaping(const std::string & value);
};

class ConnectionPostgreSQL final : public ConnectionDB
{
    int next_pos = 0;
    const bool singleRow;
    int isSingleRow;

    struct pg_conn * conn = nullptr;
    struct pg_result * res = nullptr;

    unsigned int stmtCounter = 0;
    std::string stmtName;

    int boundCount;
    std::map<int, std::string> bound;

    void clearResurce() override;
    bool firstSingleRow();

public:
    explicit ConnectionPostgreSQL(const std::function<void(std::string_view)> & logger = nullptr, bool singleRow = true);
    ~ConnectionPostgreSQL();

    bool open(std::string_view connectionInfo) override;
    bool isOpen() const override;
    void close() override;

    bool execute(std::string_view query) override;

    bool prepare(std::string_view prepare) override;
    void bind(int pos, std::string_view value) override;
    bool exec() override;

    int fieldCount() override;
    std::string fieldName(int fieldIndex) override;
    FieldType fieldType(int fieldIndex) override;

    bool next() override;
    std::string value(int field) override;

    std::set<std::string> tables() override;
};

class ConnectionSqlite final : public ConnectionDB
{
    struct sqlite3 * db = nullptr;

    bool isPrepare, isExec, isFirst;
    std::map<int, std::string> bound;

    struct sqlite3_stmt * stmt = nullptr;

    void clearResurce() override;
    bool prepare_stmt(std::string_view query, bool prepare);

public:
    explicit ConnectionSqlite(const std::function<void(std::string_view)> & logger = nullptr);
    ~ConnectionSqlite();

    bool open(std::string_view connectionInfo) override;
    bool isOpen() const override;
    void close() override;

    bool execute(std::string_view query) override;

    bool prepare(std::string_view prepare) override;
    void bind(int pos, std::string_view value) override;
    bool exec() override;

    int fieldCount() override;
    std::string fieldName(int fieldIndex) override;
    FieldType fieldType(int fieldIndex) override;

    bool next() override;
    std::string value(int fieldIndex) override;

    std::set<std::string> tables() override;
};

class PoolPointer;

class TempConnectionDB
{
    friend class ConnectionDBPool;

    std::shared_ptr<ConnectionDB> conn;
    std::weak_ptr<PoolPointer> pointer;

    explicit TempConnectionDB();
    explicit TempConnectionDB(std::shared_ptr<ConnectionDB> && conn, const std::shared_ptr<PoolPointer> & pointer);

public:
    ~TempConnectionDB();

    explicit TempConnectionDB(TempConnectionDB & other) = delete;
    TempConnectionDB & operator = (TempConnectionDB & other) = delete;

    bool isValid() const;

    void returnToPoolDB();

    std::string error() const;

    bool isOpen() const;

    bool execute(std::string_view query);

    bool prepare(std::string_view prepare);
    void bind(int pos, std::string_view value);
    bool exec();

    int fieldCount();
    std::string fieldName(int fieldIndex);
    ConnectionDB::FieldType fieldType(int fieldIndex);

    bool next();
    std::string value(int fieldIndex);

    std::set<std::string> tables();
};

class ConnectionDBPool final
{ 
    friend class PoolPointer;
    friend class TempConnectionDB;

public:

    enum ConnectionType : unsigned char
    {
         PostgreSQL = 0,
         SQLite
    };

private:
    static std::mutex p_mutex;
    static std::map<std::string, std::shared_ptr<ConnectionDBPool>> pools;

    std::shared_ptr<PoolPointer> pointer;

    std::mutex c_mutex;
    std::condition_variable condition;
    std::queue<std::shared_ptr<ConnectionDB>> connections;

    void freeConnection(std::shared_ptr<ConnectionDB> && connection);

public:
    explicit ConnectionDBPool();

    explicit ConnectionDBPool(ConnectionDBPool & other) = delete;
    ConnectionDBPool & operator = (ConnectionDBPool & other) = delete;

    bool createPool(ConnectionType type, int poolCount, std::string_view connectionInfo, const std::function<void (std::string_view)> & logger = nullptr);
    TempConnectionDB connection();

    static bool open(std::string_view connectionName, ConnectionType type, int poolCount, std::string_view connectionInfo, const std::function<void (std::string_view)> & logger = nullptr);
    static bool isOpen(std::string_view connectionName);
    static TempConnectionDB connection(std::string_view connectionName);
    static void close(std::string_view connectionName);
};

#endif // CONNECTIONDB_H
