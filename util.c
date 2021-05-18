#include "util.h"

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