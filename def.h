#ifndef __DEF_H
#define __DEF_H
#include <stdbool.h>

// -----------------------------------------------------------------------------
//  Macros
// -----------------------------------------------------------------------------
#define MIN(a, b)	(((a) < (b)) ? (a) : (b))
#define MAX(a, b)	(((a) > (b)) ? (a) : (b))
#define SQR(x)		((x) * (x))
#define SUM(x, y)	((x) + (y))
#define DIFF(x, y)	((y) - (x))
#define SWAP(x, y)	{ int tmp=x; x=y; y=tmp; }

#define SEEK_SET 0
#define SEEK_CUR 1
#define SEEK_END 2

// -----------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------
extern const int   TOPK[];
extern const int   MAX_ROUND;
extern const int   MAXK;

extern const float MAXREAL;
extern const float MINREAL;
extern const int   MAXINT;
extern const int   MININT;
extern const int   SIZEBOOL;  
extern const int   SIZECHAR;   
extern const int   SIZEINT;  
extern const int   SIZEFLOAT;
extern const int   SIZEDOUBLE;

extern const float E;
extern const float PI; 
extern const float FLOATZERO;
extern const float ANGLE;   

extern const int   CANDIDATES;

#endif // __DEF_H
