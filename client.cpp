#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <string>
#include <random>
#include "httplib.h"

using namespace std;

atomic<int> total_requests(0);
atomic<long long> total_response_time(0);
atomic<bool> test_running(true);

#include <string>
#include <cstdlib>

string random_string(int len) {
    static const char alphanum[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    const int charSetSize = sizeof(alphanum) - 1;
    
    string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % charSetSize];
    }

    return tmp_s;
}

void print_response(const httplib::Result& res) {
    cout << "  Status: " << res->status << endl;
    cout << "  Value:  " << res->body << endl;
    cout << endl;
}

// manual operation
void run_manual_mode() {
    httplib::Client cli("http://localhost:8080");
    cli.set_connection_timeout(5, 0);
    string key, value, path;

    while (true) {
        cout << "\nManual Mode" << endl;
        cout << "1. Create/Update (POST)" << endl;
        cout << "2. Retrieve (GET)" << endl;
        cout << "3. Delete (DELETE)" << endl;
        cout << "4. Back to Main Menu" << endl;
        cout << "Enter choice: ";

        int choice;
        cin >> choice;

        if (choice == 1) { // post
            cout << "  Enter key: ";
            cin >> key;
            cout << "  Enter value: ";
            cin.ignore();
            getline(cin, value);
            
            path = "/kv/" + key;
            cout << "Sending POST /kv/" << key << "..." << endl;
            auto res = cli.Post(path.c_str(), value, "text/plain");
            print_response(res);

        } else if (choice == 2) { // get
            cout << "  Enter key: ";
            cin >> key;
            
            path = "/kv/" + key;
            cout << "Sending GET /kv/" << key << "..." << endl;
            auto res = cli.Get(path.c_str());
            print_response(res);

        } else if (choice == 3) { // delete
            cout << "  Enter key: ";
            cin >> key;

            path = "/kv/" + key;
            cout << "Sending DELETE /kv/" << key << "..." << endl;
            auto res = cli.Delete(path.c_str());
            print_response(res);

        } else if (choice == 4) {
            return; // exit manual mode
        } else {
            cout << "Invalid choice. Please try again." << endl;
        }
    }
}
//load test time
void client_thread_func(int thread_id) {
    httplib::Client cli("http://localhost:8080");
    cli.set_connection_timeout(5, 0);

    while (test_running) {
        string key = "key_" + random_string(10);
        
        auto start_time = chrono::high_resolution_clock::now();
        httplib::Result res;
        string value = "val_" + random_string(20);
        res = cli.Post(("/kv/" + key).c_str(), value, "text/plain");
        
        if (res && (res->status == 200 || res->status == 201)) {
            auto end_time = chrono::high_resolution_clock::now();
            auto d = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
            
            total_requests.fetch_add(1);
            total_response_time.fetch_add(d);
        }
    }
}

//load test
void run_load_test() {
    int num_threads, test_duration_sec;

    cout << "\nLoad Test Mode (POST only)" << endl;
    cout << "Enter number of threads: ";
    cin >> num_threads;
    cout << "Enter test duration (seconds): ";
    cin >> test_duration_sec;

    total_requests = 0;
    total_response_time = 0;
    test_running = true;

    cout << "Starting load test..." << endl;

    vector<thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(client_thread_func, i);
    }

    this_thread::sleep_for(chrono::seconds(test_duration_sec));
    test_running = false;

    for (auto& t : threads) {
        t.join();
    }
    cout << "Test Finished" << endl;

    int final_requests = total_requests.load();
    long long final_time_us = total_response_time.load();
    double avg_throughput = (double)final_requests / test_duration_sec;
    double avg_response_time = (final_requests > 0) ? (final_time_us / (double)final_requests / 1000.0) : 0.0;

    cout << endl;
    cout << "Total requests:       " << final_requests << endl;
    cout << "Average throughput:   " << avg_throughput << " requests/sec" << endl;
    cout << "Average response time: " << avg_response_time << " ms" << endl;
    cout << endl;
}

int main() {
    while (true) {
        cout << "\nClient Menu" << endl;
        cout << "1. Manual Mode (GET/POST/DELETE)" << endl;
        cout << "2. Load Test Mode (POSTs)" << endl;
        cout << "3. Exit" << endl;
        cout << "Enter choice: ";

        int choice;
        cin >> choice;

        switch (choice) {
            case 1:
                run_manual_mode();
                break;
            case 2:
                run_load_test();
                break;
            case 3:
                cout << "Exiting.." << endl;
                return 0;
            default:
                cout << "Invalid choice!" << endl;
        }
    }
}
