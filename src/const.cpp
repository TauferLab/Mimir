#include "const.h"

int oneintlen = sizeof(int);
int twointlen = 2*sizeof(int);
int threeintlen = 3*sizeof(int);

int kalign=ALIGNK;
int valign=ALIGNV;
int talign=MAX(kalign, valign);


int kalignm=kalign-1;
int valignm=valign-1;
int talignm=talign-1;

