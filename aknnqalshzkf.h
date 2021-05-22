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

float update_radius(float old_radius, const float *query_object_hashes, int m_, float ratio_, float w_);

float init_radius(const float *query_object_hashes, int m_, float ratio_, float w_);

void free_matrix(float ** matrix, int m);

const char * LONG_SQL_UPDATE_RADIUS = 
"WITH CTE\n\
    AS (\n\
        (\n\
            SELECT *\n\
            FROM datarephash_%d\n\
            WHERE rephash >= %f\n\
            ORDER BY rephash ASC\n\
            LIMIT 1\n\
        )\n\
        UNION ALL\n\
        (\n\
            SELECT *\n\
            FROM datarephash_%d\n\
            WHERE rephash <= %f\n\
            ORDER BY rephash DESC\n\
            LIMIT 1\n\
        )\n\
    ) \n\
SELECT ABS(rephash - (%f)) \n\
FROM   CTE \n\
ORDER  BY ABS(rephash - (%f)) ASC LIMIT 1;";

#define __DEBUG

#endif