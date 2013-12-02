#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_reduce.h"
#include <iostream>
#include <vector>
#include <ctime>
#include <cstdlib>
#include <cstdio>
#include <sys/time.h>

#define N 150000000
#define MESUREMENTS 50

using namespace tbb;

struct Sum {
    int value;
    Sum() : value(0) {}
    Sum( Sum& s, split ) {value = 0;}
  void operator()( const blocked_range<int>& r ) {
  //void operator()(int begin, i0nt end) {
     int temp = 0;
     int begin = r.begin();
     int end = r.end();
        for( int a=begin; a!= end; ++a ) {
            temp += a;
        }
        value = temp;
    }
    void join( Sum& rhs ) {value += rhs.value;}
};

int ParallelSum(size_t n ) {
    Sum total;
    parallel_reduce( blocked_range<int>( 0, n), 
                     total );
    return total.value;
}

int main(int,char**) {

  tbb::task_scheduler_init init(1);
  int sz = N;
  
 struct timeval time;
  gettimeofday(&time, NULL);  
  double totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000.0);

  int r;
  for(int i = 0 ; i< MESUREMENTS; i++) {
    r += ParallelSum(sz);
    //printf("%i\n", i);
  }
 gettimeofday(&time, NULL);  //END-TIME
 totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000.0)) - totalTime);

 printf("%i, %lf, %lf\n", 0, totalTime/MESUREMENTS, totalTime);
  return 0;
}
