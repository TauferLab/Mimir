#include "callbacks.h"

#include "inputstream.h"

using namespace MIMIR_NS;

bool wordsplitcb(InputStream* in, void *ptr){
    const char *str = (const char*)ptr;
    char c;

    int ret = in->get_char(c);
    if(!ret || ret == EOF) return true;

    int len = (int)strlen(str)+1;
    for(int i = 0; i < len; i++){
        if(c == str[i]) return true;
    }

    return false;
}

