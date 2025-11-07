#include <iostream>
#include <string>
#include <map>
#include <mutex>
#include <memory>

#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

#include "httplib.h"

using namespace std;

map<string, string> cache;
mutex cache_mutex;
sql::Connection *con;

string parse_key(const string& path) {
    if (path.rfind("/kv/", 0) == 0) {
        return path.substr(4);
    }
    return "";
}

void handle_get(const httplib::Request& req, httplib::Response& res) {
    string key = parse_key(req.path);
    cout << "GET: " << key << endl;

    cache_mutex.lock();
    auto it = cache.find(key);
    if (it != cache.end()) {
        cout << "  [Cache HIT]" << endl;
        res.set_content(it->second, "text/plain");
        cache_mutex.unlock();
        return;
    }
    cache_mutex.unlock();

    cout << "  [Cache MISS] -> DB" << endl;
    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("SELECT id_value FROM kv_store WHERE id_key = ?")
    );
    pstmt->setString(1, key);
    unique_ptr<sql::ResultSet> db_res(pstmt->executeQuery());

    if (db_res->next()) {
        string val = db_res->getString("id_value");
        cache_mutex.lock();
        cache[key] = val;
        cache_mutex.unlock();
        res.set_content(val, "text/plain");
    } else {
        res.status = 404;
        res.set_content("Not Found", "text/plain");
    }
}

void handle_create(const httplib::Request& req, httplib::Response& res) {
    string key = parse_key(req.path);
    string value = req.body;
    cout << "POST: " << key << endl;

    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("INSERT INTO kv_store (id_key, id_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE id_value = ?")
    );
    pstmt->setString(1, key);
    pstmt->setString(2, value);
    pstmt->setString(3, value);
    pstmt->executeUpdate();
    cout << "  [DB Stored]" << endl;

    cache_mutex.lock();
    cache[key] = value;
    cache_mutex.unlock();
    cout << "  [Cache Stored]" << endl;

    res.status = 201;
    res.set_content("Created", "text/plain");
}

void handle_delete(const httplib::Request& req, httplib::Response& res) {
    string key = parse_key(req.path);
    cout << "DELETE: " << key << endl;

    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("DELETE FROM kv_store WHERE id_key = ?")
    );
    pstmt->setString(1, key);
    pstmt->executeUpdate();
    cout << "  [DB Deleted]" << endl;

    cache_mutex.lock();
    cache.erase(key);
    cache_mutex.unlock();
    cout << "  [Cache Deleted]" << endl;

    res.set_content("Deleted", "text/plain");
}

int main() {
    sql::Driver *driver = get_driver_instance();
    con = driver->connect("tcp://127.0.0.1:3306", "ashu", "ProjectPass@123");
    con->setSchema("kv_db");
    cout << "Database connection successful." << endl;

    httplib::Server svr;
    svr.new_task_queue = [] { return new httplib::ThreadPool(12); };

    svr.Get(R"(/kv/(.*))", handle_get);
    svr.Post(R"(/kv/(.*))", handle_create);
    svr.Delete(R"(/kv/(.*))", handle_delete);

    cout << "Starting server on http://localhost:8080..." << endl;
    svr.listen("0.0.0.0", 8080);
    
    delete con;
    return 0;
}