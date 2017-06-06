#ifndef MIMIR_TYPE_MODE_H
#define MIMIR_TYPE_MODE_H

template <class T>
std::string
type_name()
{
    typedef typename std::remove_reference<T>::type TR;
    std::unique_ptr<char, void(*)(void*)> own
        (
#ifndef _MSC_VER
         abi::__cxa_demangle(typeid(TR).name(), nullptr,
                             nullptr, nullptr),
#else
         nullptr,
#endif
         std::free
        );
    std::string r = own != nullptr ? own.get() : typeid(TR).name();
    if (std::is_const<TR>::value)
        r += " const";
    if (std::is_volatile<TR>::value)
        r += " volatile";
    if (std::is_lvalue_reference<T>::value)
        r += "&";
    else if (std::is_rvalue_reference<T>::value)
        r += "&&";
    return r;
}

enum TypeMode {TypeNULL, TypeString, TypeFixed};

template <typename Type>
TypeMode type_mode () {
    // The type is a class
    //if (std::is_class<Type>::value) return TypeFixed;

    // The type is not a class
    Type tmp;
    std::string typestr = type_name<decltype(tmp)>();
    // void represent empty type
    if (typestr == "void") {
        return TypeNULL;
    // char* and const char * represent string terminated with 0
    } else if (typestr == "char*" || typestr == "const char*") {
        return TypeString;
        // all other types are viewed as fixed type
    } else {
        return TypeFixed;
    }
}


#endif
