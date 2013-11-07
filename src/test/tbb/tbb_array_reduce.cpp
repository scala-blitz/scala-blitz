#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_reduce.h"
#include <iostream>
#include <vector>
#include <ctime>

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

  tbb::task_scheduler_init init(tbb::task_scheduler_init::automatic);
  std::vector<int> data;
  int sz = N;
  for (int i=0;i<sz;++i)
    data.push_back(i);
  clock_t begin = clock();
  int r;
  for(int i = 0 ; i< MESUREMENTS; i++) {
  r += ParallelSum(&data[0], sz);
  }
  clock_t end = clock();
  double elapsed_msecs = double(1000*end - 1000*begin) / CLOCKS_PER_SEC/MESUREMENTS;

  std::cout << r << ' ' << elapsed_msecs << std::endl;
  /*  std::vector<mytask> tasks;
  for (int i=0;i<100000;++i)
    tasks.push_back(mytask(i));

  executor exec(tasks);
  tbb::parallel_for(tbb::blocked_range<size_t>(0,tasks.size()),exec);
  std::cerr << std::endl;*/

  return 0;
}
