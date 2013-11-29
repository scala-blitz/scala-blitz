#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_reduce.h"
#include <iostream>
#include <vector>
#include <ctime>
#include <cmath>
#include <cstdio>
#include <sys/time.h>
#define N 500000
#define MEASUREMENTS 10

using namespace tbb;

int op(int, int);

struct Sum {
  int value;
  Sum(): value(0) {}
  Sum(Sum& s, split) { value = 0; }

  void operator()( const blocked_range<int>& r ) {
    int temp = 0;
     int begin = r.begin();
     int end = r.end();
    for (int a = begin; a != end; ++a) {
	    temp += op(a, N);
	    asm("");
    }
    value = temp;
  }

  void join(Sum& rhs) {
    value += rhs.value;
  }

};

int ParallelSum(size_t n) {
    Sum total;
      tbb::task_scheduler_init init(2);
    parallel_reduce(blocked_range<int>( 0, n ), total);
    //total(blocked_range<int>(0, n));
    return total.value;
}

int op(int e, int size) {
  int until = 1<< (e/30000);
  int acc = 1;
  int i = 1;
  while (i < until) {
    acc *= i;
    i += 1;
    //asm("");
  }
  return acc;
}

int ourloop(int begin, int end) {
  int temp = 0;
  int i = begin;
  while (i < end) {
    temp += op(i, end);
    i += 1;
    asm("");
  }
  return temp;
}

int main(int,char**) {
  int r;
  struct timeval time;
  
  gettimeofday(&time, NULL); // Start Time
  long totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000);

  for(int i = 0 ; i < MEASUREMENTS; i++) {
    r += ParallelSum(N);
    std::printf("%i\n", i);
  }

  gettimeofday(&time, NULL);  //END-TIME
  totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000)) - totalTime);

  printf("%i, %li ms\n", r, totalTime/MEASUREMENTS);

  return 0;
}


