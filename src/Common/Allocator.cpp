/*
This source file just includes overrides of new and delete operators from mimalloc library.
See https://github.com/microsoft/mimalloc.
*/

#ifdef PHJ_USE_MIMALLOC
#include "mimalloc-new-delete.h"
#endif