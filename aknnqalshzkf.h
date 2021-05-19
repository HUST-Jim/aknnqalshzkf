#ifndef __AKNNQALSHZKF_H
#define __AKNNQALSHZKF_H

// -----------------------------------------------------------------------------
//  long SQLs
// -----------------------------------------------------------------------------


struct Data
{
    int size_of_float;
    int n;
    int d;
    float **matrix;
};

struct KnnResult
{
	float key; // aka hash value
	int   id;
};

struct MinK_List
{
    int    k_;						// max numner of keys
	int    num_;					// number of key current active
	struct KnnResult *list_;		// the list itself
};

#endif