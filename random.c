#include "random.h"
#include <math.h>
#include <stdlib.h>

// -----------------------------------------------------------------------------
//  functions used for generating random variables (r.v.).
//
//  use Box-Muller transform to generate a r.v. from Gaussian(mean, sigma)
//  standard Gaussian distr. is Gaussian(0, 1), where mean = 0 and sigma = 1
// -----------------------------------------------------------------------------
float gaussian(						// r.v. from N(mean, sigma)
	float mu,							// mean (location)
	float sigma)						// stanard deviation (scale > 0)
{
	float u1 = -1.0f;
	float u2 = -1.0f;
	do {
		u1 = uniform(0.0f, 1.0f);
	} while (u1 < FLOATZERO);
	u2 = uniform(0.0f, 1.0f);

	return mu + sigma * sqrt(-2.0f * log(u1)) * cos(2.0f * PI * u2);
}

// -----------------------------------------------------------------------------
//  functions used for generating random variables (r.v.)
// -----------------------------------------------------------------------------
inline float uniform(				// r.v. from Uniform(min, max)
	float min,							// min value
	float max)							// max value
{
	return min + (max - min) * (float)rand() / (float)RAND_MAX;
}

// -----------------------------------------------------------------------------
inline float new_gaussian_prob(			// calc new gaussian probability
	float x)							// x = w / (2 * r)
{
	return new_gaussian_cdf(x, 0.001F);
}

// -----------------------------------------------------------------------------
float new_gaussian_cdf(				// cdf of N(0, 1) in range [-x, x]
	float x,							// integral border (x > 0)
	float step)							// step increment
{
	// assert(x > 0.0f);
	float ret = 0.0f;
	for (float i = -x; i <= x; i += step) {
		ret += step * gaussian_pdf(i);
	}
	return ret;
}

// -----------------------------------------------------------------------------
//  functions used for calculating probability distribution function (pdf) and 
//  cumulative distribution function (cdf)
// -----------------------------------------------------------------------------
inline float gaussian_pdf(			// pdf of N(0, 1)
	float x)							// variable
{
	return exp(-x * x / 2.0f) / sqrt(2.0f * PI);
}
