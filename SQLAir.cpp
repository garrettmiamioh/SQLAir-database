/*
 * A very lightweight (light as air) implementation of a simple CSV-based
 * database system that uses SQL-like syntax for querying and updating the
 * CSV files.
 *
 * Copyright (C) lavertgt@miamioh.edu
 */

#include "SQLAir.h"

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>

#include "HTTPFile.h"

using namespace boost::asio;
using namespace boost::asio::ip;
using namespace std;

/**
 * A fixed HTTP response header that is used by the runServer method below.
 * Note that this a constant (and not a global variable)
 */
const std::string HTTPRespHeader =
    "HTTP/1.1 200 OK\r\n"
    "Server: localhost\r\n"
    "Connection: Close\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: ";

// Called by selectQuery() and handles the process of selecting rows
// Returns the number of rows selected
int SQLAir::processSelectRow(StrVec& colNames, std::string& toPrint,
                CSV& csv, const int& whereColIdx, const std::string& cond,
                const std::string& value) {
    int count = 0;
    CSVRow printRow;
    for (auto& row : csv) {
        {  // Critical section for creating a row copy
        std::scoped_lock<std::mutex> lock(row.rowMutex);
        printRow = row;
        }
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        if (whereColIdx == -1 ||
            SQLAirBase::matches(printRow.at(whereColIdx), cond, value)) {
            std::string delim = "";
            for (const auto& colName : colNames) {
                toPrint += delim + printRow.at(csv.getColumnIndex(colName));
                delim = "\t";
            }
            toPrint += "\n";
            count++;
        }
    }
    return count;
}

// API method to perform operations associated with a "select" statement
// to print columns that match an optional condition.
void SQLAir::selectQuery(CSV& csv, bool mustWait, StrVec colNames,
                         const int whereColIdx, const std::string& cond,
                         const std::string& value, std::ostream& os) {
    // Convert any "*" to suitable column names. See CSV::getColumnNames()
    // First print the column names.
    if (colNames.at(0) == "*") {
        colNames = csv.getColumnNames();
    }
    
    std::string toPrint = "";
    // Print each row that matches an optional condition.
    int count = processSelectRow(colNames, toPrint, csv,
                                 whereColIdx, cond, value);
    if (mustWait && count < 1) {
        while (count < 1) {
            std::unique_lock<std::mutex> lock(csv.csvMutex);
            csv.csvCondVar.wait(lock);
            count = processSelectRow(colNames, toPrint, csv,
                                     whereColIdx, cond, value);
        }
    }
    if (count > 0) {
        os << colNames << "\n" << toPrint;
    }
    os << count << " row(s) selected." << std::endl;
}

// Called by updateQuery() and handles the process of updating rows
// Returns the number of rows updated
int SQLAir::processUpdateRow(CSV& csv, const int& whereColIdx, StrVec& colNames,
                            const std::string& cond, const std::string& value,
                            StrVec& values) {
    int count = 0;
    for (auto& row : csv) {
        // Start lock here to keep row.at() MT safe
        std::scoped_lock<std::mutex> lock(row.rowMutex);
        if (whereColIdx == -1 ||
            SQLAirBase::matches(row.at(whereColIdx), cond, value)) {
            for (size_t i = 0; i < colNames.size(); i++) {
                row.at(csv.getColumnIndex(colNames[i])) = values[i];
            }
            count++;
        }
    }
    return count;
}

// Allows changes to be made to csv values
void SQLAir::updateQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    // Update each row that matches an optional condition.
    // First print the column names.
    if (colNames.at(0) == "*") {
        colNames = csv.getColumnNames();
    }

    int count = processUpdateRow(csv, whereColIdx, colNames, cond, value,
                                 values);
    if (mustWait && count < 1) {
        while (count < 1) {
            std::unique_lock<std::mutex> lock(csv.csvMutex);
            csv.csvCondVar.wait(lock);
            count = processUpdateRow(csv, whereColIdx, colNames, cond, value,
                                     values);
        }
    }
    if (count > 0) {
        csv.csvCondVar.notify_all();
    }
    os << count << " row(s) updated." << std::endl;
}

// This method allows threads to process queries and load files
void SQLAir::serveClient(std::istream& is, std::ostream& os) {
    std::ostringstream resp;
    std::string useless, line, toPrint, tmp;
    is >> useless >> line;
    while (std::getline(is, tmp) && (tmp != "\r")) {
    }
    if (line.find('?') != std::string::npos) {
        line = Helper::url_decode(line);
        line = line.substr(line.find('=') + 1);

        try {
            process(line, resp);
        } catch (const std::exception& exp) {
            resp << "Error: " << exp.what() << std::endl;
        }

        toPrint = resp.str();
        std::string output = HTTPRespHeader + to_string(toPrint.length())
                             + "\r\n\r\n" + toPrint;
        os << output;

    } else if (!line.empty()) {
        line = line.substr(1);  // Remove the leading '/' sign.
        os << http::file(line);
    }
    SQLAir::numThreads--;
    SQLAir::thrCond.notify_one();
}

// The method to have this class run as a web-server.
void SQLAir::runServer(boost::asio::ip::tcp::acceptor& server,
                       const int maxThr) {
    std::mutex serverMutex;
    // Process client connections one-by-one
    while (true) {
        // Create a client stream
        auto client = std::make_shared<tcp::iostream>();
        // Wait for a client to connect
        server.accept(*client->rdbuf());
        // After client connects, ensure proper thread count before proceeding
        std::unique_lock<std::mutex> lock(serverMutex);
        SQLAir::thrCond.wait(lock,
        [this, maxThr]() { return SQLAir::numThreads < maxThr; });

        std::thread thr([this, client]() { serveClient(*client, *client); });
        thr.detach();
        SQLAir::numThreads++;
    }
}

// Loads data from server responses while checking for a good connection and
// a '200 ok' response.
void SQLAir::setupDownload(const std::string& hostName, const std::string& path,
                           tcp::iostream& data,
                           const std::string& port = "80") {
    // Create a boost socket and request the log file from the server.
    data.connect(hostName, port);
    data << "GET " << path << " HTTP/1.1\r\n"
         << "Host: " << hostName << "\r\n"
         << "Connection: Close\r\n\r\n";
    if (!data.good()) {
        throw Exp("Unable to connect to " + hostName + " at port " + port);
    }
    std::string line;
    std::getline(data, line);
    if (line.find("200 OK") == std::string::npos) {
        throw Exp("Error (" + Helper::trim(line) + ") getting " + path +
                  " from " + hostName + " at port " + port);
    }
    for (std::string str;
         std::getline(data, str) && str != "" && str != "\r";) {
    }
}

// Helper method to obtain a reference to a pre-loaded CSV file from the
// inMemoryCSV map.  If the requested file is not present, then this
// method loads the data into the inMemoryCSV.
CSV& SQLAir::loadAndGet(std::string fileOrURL) {
    // Check if the specified fileOrURL is already loaded in a thread-safe
    // manner to avoid race conditions on the unordered_map
    {
        std::scoped_lock<std::mutex> guard(recentCSVMutex);
        // Use recent CSV if parameter was empty string.
        fileOrURL = (fileOrURL.empty() ? recentCSV : fileOrURL);
        // Update the most recently used CSV for the next round
        recentCSV = fileOrURL;
        if (inMemoryCSV.find(fileOrURL) != inMemoryCSV.end()) {
            // Requested CSV is already in memory. Just return it.
            return inMemoryCSV.at(fileOrURL);
        }
    }
    // When control drops here, we need to load the CSV into memory.
    // Loading or I/O is being done outside critical sections
    CSV csv;  // Load data into this csv
    if (fileOrURL.find("http://") == 0) {
        // This is an URL. We have to get the stream from a web-server
        // Implement this feature.
        std::string host, port, path;
        std::tie(host, port, path) = Helper::breakDownURL(fileOrURL);
        tcp::iostream data;
        setupDownload(host, path, data, port);
        csv.load(data);
    } else {
        // We assume it is a local file on the server. Load that file.
        std::ifstream data(fileOrURL);
        // This method may throw exceptions on errors.
        csv.load(data);
    }

    // We get to this line of code only if the above if-else to load the
    // CSV did not throw any exceptions. In this case we have a valid CSV
    // to add to our inMemoryCSV list. We need to do that in a thread-safe
    // manner.
    std::scoped_lock<std::mutex> guard(recentCSVMutex);
    // Move (instead of copy) the CSV data into our in-memory CSVs
    inMemoryCSV[fileOrURL].move(csv);
    // Return a reference to the in-memory CSV (not temporary one)
    return inMemoryCSV.at(fileOrURL);
}

// Save the currently loaded CSV file to a local file.
void SQLAir::saveQuery(std::ostream& os) {
    if (recentCSV.empty() || recentCSV.find("http://") == 0) {
        throw Exp("Saving CSV to an URL using POST is not implemented");
    }
    // Create a local file and have the CSV write itself.
    std::ofstream csvData(recentCSV);
    inMemoryCSV.at(recentCSV).save(csvData);
    os << recentCSV << " saved.\n";
}
