#include "util.h"
#include "random.h"
#include "postgres.h"
#include "fmgr.h"

#include <math.h>


/* 返回给定对象在数组中的位置，没有找到则返回 -1 */
int
num_in_array(int num, int array[], int array_len)
{
    for (int i = 0; i < array_len; i++)
    {
        if (array[i] == num)
            return i;
    }
    return -1;
}

float 
calc_inner_product(			// calc inner product
	int   dim,							// dimension
	const float *p1,					// 1st point
	const float *p2)					// 2nd point
{
	float r = 0.0f;
	for (int i = 0; i < dim; ++i) 
    {
		r += p1[i] * p2[i];
	}
	return r;
}

float
calc_l2_dist(// fix me if number is too big for square
	int 		dim,
	const float *p1,
	const float *p2)
{
	float *tmp = palloc(sizeof(float) * dim);
	float result;
	for (int i = 0; i < dim; i++)
	{
		tmp[i] = p1[i] - p2[i];
		//elog(INFO, "p1[%d] = %f", i, p1[i]);
		//elog(INFO, "p2[%d] = %f", i, p2[i]);
		//elog(INFO, "tmp[%d] = %f", i, tmp[i]);
	}
	result = calc_inner_product(dim, tmp, tmp);
	//elog(INFO, "inner product = %f", result);
	pfree(tmp);
	return sqrt(result);
}

float 
calc_l2_prob(	// calc prob <p1_> and <p2_> of L2 dist
	float x)							// x = w / (2.0 * r)
{
	return new_gaussian_prob(x);
}

int 
compare_float_helper(const void *a, const void *b) // needed by qsort
{
    float c = *(float *)a;
    float d = *(float *)b;
    if (c < d)
		return -1;
	else if (c == d)
		return 0;
    else 
		return 1;
}

