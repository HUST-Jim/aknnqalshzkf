#include "util.h"
#include "random.h"

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
calc_l2_prob(	// calc prob <p1_> and <p2_> of L2 dist
	float x)							// x = w / (2.0 * r)
{
	return new_gaussian_prob(x);
}