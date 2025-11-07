#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <string>
#include <random>
#include "httplib.h"

using namespace std;

const int NUM_THREADS = 4;
const int TEST_DURATION_SEC = 10;
const string WORKLOAD = "put_all";

atomic<int> total_requests(0);
atomic<long long> total_response_time_us(0);
atomic<bool> test_running(true);

string random_string(int len) {
    static const char alphanum[] = "0123456789abcdef";
    string tmp_s;
    tmp_s.reserve(len);
    static thread_local mt19937 gen(random_device{}());
    uniform_int_distribution<int> dist(0, sizeof(alphanum) - 2);
    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[dist(gen)];
    }
    return tmp_s;
}

void client_thread_func(int thread_id) {
    httplib::Client cli("http://localhost:8080");
    cli.set_connection_timeout(5, 0);

    cout << "Client thread " << thread_id << " started." << endl;

    while (test_running) {
        string key = "key_" + random_string(10);
        
        auto start_time = chrono::high_resolution_clock::now();
        httplib::Result res;

        if (WORKLOAD == "put_all") {
            string value = "val_" + random_string(20);
            res = cli.Post(("/kv/" + key).c_str(), value, "text/plain");
        } else {
            res = cli.Get(("/kv/" + key).c_str());
        }

        if (res && (res->status == 200 || res->status == 201)) {
            auto end_time = chrono::high_resolution_clock::now();
            auto duration_us = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
            
            total_requests.fetch_add(1);
            total_response_time_us.fetch_add(duration_us);
        }
    }
}

int main() {
    cout << "Starting load generator:" << endl;
    cout << "  Threads: " << NUM_THREADS << endl;
    cout << "  Duration: " << TEST_DURATION_SEC << "s" << endl;

    vector<thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(client_thread_func, i);
    }

    this_thread::sleep_for(chrono::seconds(TEST_DURATION_SEC));
    test_running = false;
    cout << "Stopping threads" << endl;

    for (auto& t : threads) {
        t.join();
    }
    cout << "Test Finished" << endl;

    int final_requests = total_requests.load();
    long long final_time_us = total_response_time_us.load();
    double avg_throughput = (double)final_requests / TEST_DURATION_SEC;
    double avg_response_time_ms = (final_requests > 0) ? (final_time_us / (double)final_requests / 1000.0) : 0.0;

    cout << "Total requests:       " << final_requests << endl;
    cout << "Average throughput:   " << avg_throughput << " reqs/sec" << endl;
    cout << "Average response time: " << avg_response_time_ms << " ms" << endl;
    return 0;
}