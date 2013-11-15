#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>
#include <sys/time.h>
#define N 1000000000
#define MESUREMENTS 300
#define NUM_THREADS 12

int main (int argc, char *argv[]) 
{
int   i, n, sum;
int*  a; 
 
 a = malloc(N*sizeof(int));
 sum = 0;
 struct timeval time;
 gettimeofday(&time, NULL);  //END-TIME
 double totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000.0);
  
 int t;
 for(t = 0 ; t< MESUREMENTS; t++) {
omp_set_num_threads(NUM_THREADS);
#pragma omp parallel for reduction(+:sum)  
   for (i=0; i < N; i++)
     sum = sum + a[i];
 }
 gettimeofday(&time, NULL);  //END-TIME
 totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000.0)) - totalTime);

  printf("%i, %lf\n",i, totalTime/MESUREMENTS);


}
