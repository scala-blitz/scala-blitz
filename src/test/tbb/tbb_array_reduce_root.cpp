#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_reduce.h"
#include <iostream>
#include <vector>
#include <ctime>
#include <cmath>
#include <cstdio>
#define N 3000000
#define MESUREMENTS 10

using namespace tbb;

struct Sum {
    int value;
    Sum() : value(0) {}
    Sum( Sum& s, split ) {value = 0;}
  int consumeTime(int value) {
    volatile int acc = 1;
    int until = std::sqrt(value);
    for(int i = 0; i< until; i++){
      asm("");
      acc = (acc * i) / 3;
      asm("");
      // std::cout<<acc<<std::endl;
    }
    return acc;
  }
    void operator()( const blocked_range<int>& r ) {
        int temp = value;
	int init = r.begin();
	int end = r.end();

        for( int a=r.begin(); a!=r.end(); ++a ) {

	    temp += consumeTime(a);

        }
        value = temp;
    }
    void join( Sum& rhs ) {value += rhs.value;}

};



int ParallelSum( size_t n ) {
    Sum total;
    parallel_reduce( blocked_range<int>( 0, n ), 
                       total );
    total(blocked_range<int>( 0, n ));
    return total.value;
}

int main(int,char**) {

  int sz = N;
  int r;
  clock_t begin = clock();
  for(int i = 0 ; i< MESUREMENTS; i++) {
  r += ParallelSum(sz);
  std::printf("%i\n", i);
  }
  clock_t end = clock();
  double elapsed_msecs = double(1000*end - 1000*begin) / CLOCKS_PER_SEC/MESUREMENTS;

  std::cout << r << ' ' << elapsed_msecs << std::endl;

  return 0;
}
