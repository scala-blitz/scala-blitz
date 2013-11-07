#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>
#define N 50000000
#define MESUREMENTS 300
#define NUM_THREADS 4

int main (int argc, char *argv[]) 
{
int   i, n, sum;
int*  a; 
//omp_set_num_threads(NUM_THREADS);
a = malloc(N*sizeof(int));
 sum = 0;
  clock_t begin = clock();
  int t;
  for(t = 0 ; t< MESUREMENTS; t++) {
	#pragma omp parallel for reduction(+:sum)  
	  for (i=0; i < N; i++)
	    sum = sum + a[i];
  }
  clock_t end = clock();
  double elapsed_msecs = (1000*end - 1000*begin) *1.0f / CLOCKS_PER_SEC/MESUREMENTS;
  printf("time: %lf\n", elapsed_msecs);

}
