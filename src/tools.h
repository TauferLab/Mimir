#ifndef MIMIR_VAR_128_INT_H
#define MIMIR_VAR_128_INT_H

#include <type_traits>
#include <typeinfo>
#ifndef _MSC_VER
#   include <cxxabi.h>
#endif
#include <memory>
#include <string>
#include <cstdlib>

#include "interface.h"

using namespace MIMIR_NS;

int encode_varint(char *buf, uint64_t x);
uint64_t decode_varint(char *buf);

inline void mimir_copy (Readable *input, Writable *output, void *ptr) {
    BaseRecordFormat *record = NULL;
    while ((record = input->read()) != NULL) {
        output->write(record);
    }
}


inline int encode_varint(char *buf, uint64_t x)
{
    int n;

    n = 0;

    while (x > 127) {
        buf[n++] = (char) (0x80 | (x & 0x7F));
        x >>= 7;
    }

    buf[n++] = (char) x;
    return n;
}

inline uint64_t decode_varint(char *buf)
{
    int      shift, n;
    uint64_t x, c;

    n = 0;
    x = 0;

    for (shift = 0; shift < 64; shift += 7) {
        c = (uint64_t) buf[n++];
        x |= (c & 0x7F) << shift;
        if ((c & 0x80) == 0) {
            break;
        }
    }

    return x;
}

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

#endif
