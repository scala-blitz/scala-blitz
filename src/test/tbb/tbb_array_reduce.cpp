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

#define N 50000000
#define MESUREMENTS 30

using namespace tbb;

struct Sum {
    int value;
    Sum() : value(0) {}
    Sum( Sum& s, split ) {value = 0;}
    void operator()( const blocked_range<int*>& r ) {
        int temp = value;
        for( int* a=r.begin(); a!=r.end(); ++a ) {
            temp += *a;
        }
        value = temp;
    }
    void join( Sum& rhs ) {value += rhs.value;}
};

int ParallelSum( int array[], size_t n ) {
    Sum total;
    parallel_reduce( blocked_range<int*>( array, array+n ), 
                     total );
    return total.value;
}

int main(int,char**) {

  tbb::task_scheduler_init init(8);
  int sz = N;
  int*  data; 
 
  data = (int *)std::malloc(N*sizeof(int));
  
 struct timeval time;
  gettimeofday(&time, NULL);  
  double totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000.0);

  int r;
  for(int i = 0 ; i< MESUREMENTS; i++) {
  r += ParallelSum(&data[0], sz);
  }
 gettimeofday(&time, NULL);  //END-TIME
 totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000.0)) - totalTime);

 printf("%i, %lf, %lf\n", 0, totalTime/MESUREMENTS, totalTime);

  /*  std::vector<mytask> tasks;
  for (int i=0;i<100000;++i)
    tasks.push_back(mytask(i));

  executor exec(tasks);
  tbb::parallel_for(tbb::blocked_range<size_t>(0,tasks.size()),exec);
  std::cerr << std::endl;*/

  return 0;
}
