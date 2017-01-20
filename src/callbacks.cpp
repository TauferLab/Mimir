#include "callbacks.h"

#include "inputstream.h"

using namespace MIMIR_NS;

bool wordsplitcb(IStream *in, void *ptr){
    const char *str = (const char*)ptr;
    int len = (int)strlen(str)+1;

    unsigned char ch = in->get_byte();
    for(int i = 0; i < len; i++){
        if(ch == str[i]) return true;
    }

    return false;
}

