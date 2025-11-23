#include <iostream>
#include <string>
#include <map>
#include <unordered_map> 
#include <list>          
#include <mutex>
#include <memory> 
#include <atomic>        // Added for thread-safe counters

#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

#include "httplib.h"

using namespace std;

const string DB_HOST = "tcp://127.0.0.1:3306";
const string DB_USER = "ashu";
const string DB_PASS = "ProjectPass@123";
const string DB_NAME = "kv_db";

atomic<long long> cache_hits(0);
atomic<long long> cache_misses(0);

// FIFO Cache Implementation
class FIFOCache {
private:
    size_t capacity;
    unordered_map<string, string> cache_map;
    list<string> key_order;
    unordered_map<string, list<string>::iterator> key_iterators;
    mutex cache_mutex;

public:
    FIFOCache(size_t cap) : capacity(cap) {}

    bool get(const string& key, string& value_out) {
        lock_guard<mutex> lock(cache_mutex);
        auto it = cache_map.find(key);
        if (it != cache_map.end()) {
            value_out = it->second;
            return true;
        }
        return false;
    }

    void put(const string& key, const string& value) {
        lock_guard<mutex> lock(cache_mutex);

        if (cache_map.find(key) != cache_map.end()) {
            cache_map[key] = value;
            return;
        }

        if (cache_map.size() >= capacity) {
            string key_to_remove = key_order.front();
            key_order.pop_front();
            cache_map.erase(key_to_remove);
            key_iterators.erase(key_to_remove);
        }

        key_order.push_back(key);
        cache_map[key] = value;
        key_iterators[key] = prev(key_order.end());
    }

    void remove(const string& key) {
        lock_guard<mutex> lock(cache_mutex);
        if (cache_map.find(key) != cache_map.end()) {
            auto it = key_iterators[key];
            key_order.erase(it);
            key_iterators.erase(key);
            cache_map.erase(key);
        }
    }
};

FIFOCache global_cache(10000);

// DB Helper
sql::Connection* get_db_connection() {
    sql::Driver *driver = get_driver_instance();
    sql::Connection *con = driver->connect(DB_HOST, DB_USER, DB_PASS);
    con->setSchema(DB_NAME);
    return con;
}

//Handlers

// GET Handler
void handle_get(const httplib::Request& req, httplib::Response& res) {
    string key = req.path_params.at("key");
    string cached_val;

    // Check Cache
    if (global_cache.get(key, cached_val)) {
        cache_hits++;
        res.set_content(cached_val, "text/plain");
        return;
    }

    // Check DB
    cache_misses++;
    // cout << "GET: " << key << endl;
    
    unique_ptr<sql::Connection> con(get_db_connection());
    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("SELECT id_value FROM kv_store WHERE id_key = ?")
    );
    pstmt->setString(1, key);
    unique_ptr<sql::ResultSet> db_res(pstmt->executeQuery());

    if (db_res->next()) {
        string val = db_res->getString("id_value");
        global_cache.put(key, val);
        res.set_content(val, "text/plain");
    } else {
        res.status = 404;
        res.set_content("Not Found", "text/plain");
    }
}

// CREATE Handler
void handle_create(const httplib::Request& req, httplib::Response& res) {
    string key = req.path_params.at("key");
    string value = req.body;
    // cout << "POST: " << key << endl;
    unique_ptr<sql::Connection> con(get_db_connection());
    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("INSERT INTO kv_store (id_key, id_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE id_value = ?")
    );
    pstmt->setString(1, key);
    pstmt->setString(2, value);
    pstmt->setString(3, value);
    pstmt->executeUpdate();

    global_cache.put(key, value);

    res.status = 201;
    res.set_content("Created", "text/plain");
}

// DELETE Handler
void handle_delete(const httplib::Request& req, httplib::Response& res) {
    string key = req.path_params.at("key");
    // cout << "DELETE: " << key << endl;

    unique_ptr<sql::Connection> con(get_db_connection());
    unique_ptr<sql::PreparedStatement> pstmt(
        con->prepareStatement("DELETE FROM kv_store WHERE id_key = ?")
    );
    pstmt->setString(1, key);
    pstmt->executeUpdate();

    global_cache.remove(key);

    res.set_content("Deleted", "text/plain");
}

// STATS Handler
void handle_stats(const httplib::Request& req, httplib::Response& res) {
    long long hits = cache_hits.load();
    long long misses = cache_misses.load();
    long long total = hits + misses;
    double rate = (total > 0) ? (double)hits / total * 100.0 : 0.0;
    
    string body = "Cache Hits: " + to_string(hits) + 
                  "\nCache Misses: " + to_string(misses) + 
                  "\nTotal Req (Get): " + to_string(total) +
                  "\nHit Rate: " + to_string(rate) + "%\n";
    
    res.set_content(body, "text/plain");
}

int main() {
    get_driver_instance();
    cout << "MySQL driver loaded successfully." << endl;
    cout << "Starting server on http://localhost:8080..." << endl;
    cout << "Use /stats to check performance metrics." << endl;

    httplib::Server svr;
    svr.new_task_queue = [] { return new httplib::ThreadPool(12); };

    svr.Get("/kv/:key", handle_get);
    svr.Post("/kv/:key", handle_create);
    svr.Delete("/kv/:key", handle_delete);
    
    svr.Get("/stats", handle_stats);

    svr.listen("0.0.0.0", 8080);
    
    return 0;
}