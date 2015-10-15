#include "log.h"

using namespace MAPREDUCE_NS; 

Log::Log(){
}

Log::~Log(){
}

void Log::output(std::string str){
    std::cout << str << std::endl;
}

void Log::output(std::string str, std::ofstream& of){
    of << str << std::endl; 
}
