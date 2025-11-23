#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <string>
#include <random>
#include <iomanip>
#include <fstream>
#include "httplib.h"

using namespace std;

atomic<long long> total_requests(0);      
atomic<long long> total_response_time(0); //in microseconds
atomic<bool> test_running(true);

enum WorkloadType {
    PUT_ALL = 1,
    GET_ALL_MISS = 2,
    GET_POPULAR = 3,
    MIXED = 4
};

string generate_random_string(int len) {
    static const char alphanum[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    static thread_local std::mt19937 generator(std::random_device{}());
    static thread_local std::uniform_int_distribution<> distribution(0, sizeof(alphanum) - 2);

    string tmp_s;
    tmp_s.reserve(len);
    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[distribution(generator)];
    }
    return tmp_s;
}

int generate_random_int(int min, int max) {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<> distribution(min, max);
    return distribution(generator);
}

void print_response(const httplib::Result& res) {
    if (res) {
        cout << "  Status: " << res->status << endl;
        cout << "  Body:   " << res->body << endl;
    } else {
        cout << "  Error: No response (Connection failed)" << endl;
    }
    cout << endl;
}

void log_to_csv(int workload_idx, int threads, int duration, long long total_reqs, double throughput, double avg_latency) {
    string filename = "load_test_results.csv";
    ofstream file;

    file.open(filename, ios::out | ios::app);

    if (!file.is_open()) {
        cerr << "[Error] Could not open " << filename << " for writing." << endl;
        return;
    }

    file.seekp(0, ios::end);
    if (file.tellp() == 0) {
        file << "Workload_Type,Threads,Duration_Sec,Total_Requests,Throughput_ReqSec,Avg_Response_Time_ms\n";
    }

    string workload_name;
    switch (workload_idx) {
        case 1: workload_name = "Put_All"; break;
        case 2: workload_name = "Get_All_Miss"; break;
        case 3: workload_name = "Get_Popular"; break;
        case 4: workload_name = "Mixed"; break;
        default: workload_name = "Unknown"; break;
    }

    file << workload_name << ","
         << threads << ","
         << duration << ","
         << total_reqs << ","
         << fixed << setprecision(2) << throughput << ","
         << fixed << setprecision(4) << avg_latency << "\n";

    file.close();
    cout << "  [Data Saved] Results appended to " << filename << endl;
}

void run_manual_mode() {
    httplib::Client cli("http://localhost:8080");
    cli.set_connection_timeout(5, 0); 
    string key, value, path;

    while (true) {
        cout << "\n--- Manual Mode ---" << endl;
        cout << "1. Create/Update (POST)" << endl;
        cout << "2. Retrieve (GET)" << endl;
        cout << "3. Delete (DELETE)" << endl;
        cout << "4. Back to Main Menu" << endl;
        cout << "Enter choice: ";

        int choice;
        if (!(cin >> choice)) {
            cin.clear(); cin.ignore(10000, '\n'); continue;
        }

        if (choice == 1) { // POST
            cout << "  Enter key: "; cin >> key;
            cout << "  Enter value: "; cin.ignore(); getline(cin, value);
            path = "/kv/" + key;
            auto res = cli.Post(path.c_str(), value, "text/plain");
            print_response(res);

        } else if (choice == 2) { // GET
            cout << "  Enter key: "; cin >> key;
            path = "/kv/" + key;
            auto res = cli.Get(path.c_str());
            print_response(res);

        } else if (choice == 3) { // DELETE
            cout << "  Enter key: "; cin >> key;
            path = "/kv/" + key;
            auto res = cli.Delete(path.c_str());
            print_response(res);

        } else if (choice == 4) {
            return;
        }
    }
}

void client_thread_func(int thread_id, WorkloadType type) {
    httplib::Client cli("http://localhost:8080");
    cli.set_connection_timeout(2, 0); 
    cli.set_read_timeout(2, 0);

    while (test_running) {
        string key, value, path;
        string method; 
        if (type == PUT_ALL) {
            key = "put_" + generate_random_string(10);
            value = "val_" + generate_random_string(50);
            method = "POST";
        } 
        else if (type == GET_ALL_MISS) {
            key = "miss_" + generate_random_string(12);
            method = "GET";
        } 
        else if (type == GET_POPULAR) {
            key = "pop_" + to_string(generate_random_int(0, 99));
            method = "GET";
        } 
        else if (type == MIXED) {
            if (generate_random_int(0, 1) == 0) {
                key = "mix_" + generate_random_string(8);
                value = "val_" + generate_random_string(20);
                method = "POST";
            } else {
                if (generate_random_int(0, 1) == 0) 
                    key = "pop_" + to_string(generate_random_int(0, 99));
                else 
                    key = "mix_" + generate_random_string(8);
                method = "GET";
            }
        }

        path = "/kv/" + key;
        
        auto start_time = chrono::high_resolution_clock::now();
        httplib::Result res;

        if (method == "POST") {
            res = cli.Post(path.c_str(), value, "text/plain");
        } else if (method == "GET") {
            res = cli.Get(path.c_str());
        }

        auto end_time = chrono::high_resolution_clock::now();

        if (res && (res->status >= 200 && res->status < 500)) {
            auto duration = chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
            total_requests.fetch_add(1);
            total_response_time.fetch_add(duration);
        } 
    }
}

// Helper to Pre-populate Popular Keys
void populate_popular_keys() {
    cout << "Pre-populating 100 'popular' keys..." << endl;
    httplib::Client cli("http://localhost:8080");
    for(int i=0; i<100; i++) {
        string key = "pop_" + to_string(i);
        string val = "popular_value_" + to_string(i);
        cli.Post(("/kv/" + key).c_str(), val, "text/plain");
    }
    cout << "done." << endl;
}

void run_load_test() {
    int num_threads, duration_sec, workload_choice;

    cout << "\n--- Load Test Configuration ---" << endl;
    cout << "Select Workload Type:" << endl;
    cout << "  1. Put All (Write Heavy)" << endl;
    cout << "  2. Get All Miss (Read Unique)" << endl;
    cout << "  3. Get Popular (Read Repeated)" << endl;
    cout << "  4. Mixed (Random Read/Write)" << endl;
    cout << "Choice: ";
    cin >> workload_choice;

    if (workload_choice < 1 || workload_choice > 4) {
        cout << "Invalid choice." << endl;
        return;
    }

    if (workload_choice == GET_POPULAR || workload_choice == MIXED) {
        populate_popular_keys();
    }

    cout << "Enter number of client threads: ";
    cin >> num_threads;
    cout << "Enter duration (seconds): ";
    cin >> duration_sec;

    total_requests = 0;
    total_response_time = 0;
    test_running = true;

    cout << "\nStarting load test with " << num_threads << " threads for " << duration_sec << " seconds..." << endl;

    vector<thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(client_thread_func, i, static_cast<WorkloadType>(workload_choice));
    }

    this_thread::sleep_for(chrono::seconds(duration_sec));
    test_running = false;

    for (auto& t : threads) {
        if(t.joinable()) t.join();
    }
    long long successful_reqs = total_requests.load();
    long long total_time_us = total_response_time.load();

    double avg_throughput = (double)successful_reqs / duration_sec;
    double avg_response_time = (successful_reqs > 0) 
        ? (double)total_time_us / successful_reqs / 1000.0 
        : 0.0;

    cout << "\n--- Test Results ---" << endl;
    cout << "Total Requests:       " << successful_reqs << endl;
    cout << "Avg Throughput:       " << fixed << setprecision(2) << avg_throughput << " req/sec" << endl;
    cout << "Avg Response Time:    " << fixed << setprecision(3) << avg_response_time << " ms" << endl;
    cout << "--------------------" << endl;

    log_to_csv(workload_choice, num_threads, duration_sec, successful_reqs, avg_throughput, avg_response_time);
}

int main() {
    srand(time(0));

    while (true) {
        cout << "\nMain Menu" << endl;
        cout << "1. Manual Mode" << endl;
        cout << "2. Load Test Mode" << endl;
        cout << "3. Exit" << endl;
        cout << "Enter choice: ";

        int choice;
        if (!(cin >> choice)) {
            cin.clear(); 
            cin.ignore(10000, '\n'); 
            continue;
        }

        switch (choice) {
            case 1:
                run_manual_mode();
                break;
            case 2:
                run_load_test();
                break;
            case 3:
                cout << "Exiting..." << endl;
                return 0;
            default:
                cout << "Invalid choice!" << endl;
        }
    }
}