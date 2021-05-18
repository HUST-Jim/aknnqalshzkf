#ifndef __UTIL_H
#define __UTIL_H

float 
calc_inner_product(			// calc inner product
	int   dim,							// dimension
	const float *p1,					// 1st point
	const float *p2);                   // 2nd point

float calc_l2_prob(float);

#endif