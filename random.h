#ifndef __RANDOM_H
#define __RANDOM_H
#include "def.h"
// -----------------------------------------------------------------------------
float gaussian(						// r.v. from Gaussian(mean, sigma)
	float mu,							// mean (location)
	float sigma);						// stanard deviation (scale > 0)

// -----------------------------------------------------------------------------
float uniform(				// r.v. from Uniform(min, max)
	float min,							// min value
	float max);

// -----------------------------------------------------------------------------
float new_gaussian_prob(			// calc new gaussian probability
	float x);							// x = w / (2 * r)

// -----------------------------------------------------------------------------
float new_gaussian_cdf(				// cdf of N(0, 1) in range [-x, x]
	float x,							// integral border (x > 0)
	float step);							// step increment

// -----------------------------------------------------------------------------
//  functions used for calculating probability distribution function (pdf) and 
//  cumulative distribution function (cdf)
// -----------------------------------------------------------------------------
float gaussian_pdf(			// pdf of N(0, 1)
	float x);							// variable


#endif // __RANDOM_H

