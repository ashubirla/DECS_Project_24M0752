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

const string DB_HOST = "tcp://127.0.0.1:3306";
const string DB_USER = "ashu";
const string DB_PASS = "ProjectPass@123";
const string DB_NAME = "kv_db";

sql::Connection* get_db_connection() {
    sql::Driver *driver = get_driver_instance();
    sql::Connection *con = driver->connect(DB_HOST, DB_USER, DB_PASS);
    con->setSchema(DB_NAME);
    return con;
}


void handle_get(const httplib::Request& req, httplib::Response& res) {
    unique_ptr<sql::Connection> con(get_db_connection());

    string key = req.path_params.at("key");


    cache_mutex.lock();
    auto it = cache.find(key);
    if (it != cache.end()) {
        cout<<"GOT FROM CACHE for key "<<key<<" :"<< cache[key]<<endl;
        res.set_content(it->second, "text/plain");
        cache_mutex.unlock();
        return;
    }
    cache_mutex.unlock();
    cout << "GET: " << key << endl;
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
    unique_ptr<sql::Connection> con(get_db_connection());

    string key = req.path_params.at("key");
    string value = req.body;
    cout << "POST: " << key << endl;

    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("INSERT INTO kv_store (id_key, id_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE id_value = ?")
    );
    pstmt->setString(1, key);
    pstmt->setString(2, value);
    pstmt->setString(3, value);
    pstmt->executeUpdate();

    cache_mutex.lock();
    cache[key] = value;
    cache_mutex.unlock();

    res.status = 201;
    res.set_content("Created", "text/plain");
}

void handle_delete(const httplib::Request& req, httplib::Response& res) {
    unique_ptr<sql::Connection> con(get_db_connection());

    string key = req.path_params.at("key");
    cout << "DELETE: " << key << endl;

    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("DELETE FROM kv_store WHERE id_key = ?")
    );
    pstmt->setString(1, key);
    pstmt->executeUpdate();

    cache_mutex.lock();
    cache.erase(key);
    cache_mutex.unlock();

    res.set_content("Deleted", "text/plain");
}

int main() {
    get_driver_instance();
    cout << "MySQL driver loaded successfully." << endl;

    httplib::Server svr;
    svr.new_task_queue = [] { return new httplib::ThreadPool(12); };

    svr.Get("/kv/:key", handle_get);
    svr.Post("/kv/:key", handle_create);
    svr.Delete("/kv/:key", handle_delete);

    cout << "Starting server on http://localhost:8080..." << endl;
    svr.listen("0.0.0.0", 8080);
    
    return 0;
}
