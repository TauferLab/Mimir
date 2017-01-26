#include "callbacks.h"

//#include "filereader.h"

//using namespace MIMIR_NS;

#if 0
bool wordsplitcb(FileReader *in, void *ptr){

    const char *str = (const char*)ptr;
    int len = (int)strlen(str)+1;

    char ch = *(in->get_byte());
    for(int i = 0; i < len; i++){
        if(ch == str[i]) return true;
    }

    return false;
}
#endif
