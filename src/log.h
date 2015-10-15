#ifndef LOG_H
#define LOG_H

#include <iostream>
#include <sstream>
#include <fstream>
#include <string>

namespace MAPREDUCE_NS {

class Log{
public:
    Log();
    ~Log();
public:
    static void output(std::string str);
    static void output(std::string str, std::ofstream& of);
};

}
#endif
