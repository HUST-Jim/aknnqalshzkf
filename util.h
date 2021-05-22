#ifndef __UTIL_H
#define __UTIL_H

float calc_inner_product(			// calc inner product
	int   dim,							// dimension
	const float *p1,					// 1st point
	const float *p2);                   // 2nd point

float calc_l2_prob(float);

float calc_l2_dist(int 		dim, const float *p1, const float *p2);

int compare_float_helper(const void *a, const void *b); // needed by qsort

int num_in_array(int num, int array[], int array_len);
#endif