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
  //void operator()(int begin, i0nt end) {
    int temp = 0;
     int begin = r.begin();
     int end = r.end();
		//if (end<init) std::printf("AAAAAAAAAAAAworking on %d %d size %d. pos %lf\n", init , end ,  end-init, init * 1.0/N);
		//else 
//     std::printf("working on %d %d size %d. pos %lf\n", begin , end ,  end-begin, begin * 1.0/N);

    //for(int a = r.begin(); a != r.end(); ++a) {
    for (int a = begin; a != end; ++a) {
	    temp += op(a, N);
	    asm("");
	    // std::cout<<std::endl;
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
    //total(0, n);
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
  //for (int a = begin; a != end; ++a) {
  int i = begin;
  while (i < end) {
    temp += op(i, end);
    i += 1;
    asm("");
  }
  //value = temp;
  return temp;
}

int main(int,char**) {
  int r;
  struct timeval time;
  
  gettimeofday(&time, NULL); // Start Time
  long totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000);

  for(int i = 0 ; i < MEASUREMENTS; i++) {
    r += ParallelSum(N);
    //r += ourloop(0, N);
    std::printf("%i\n", i);
  }

  gettimeofday(&time, NULL);  //END-TIME
  totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000)) - totalTime);

  /*    for(int j = 0; j < i * 10000000; j++) { printf("1"); }*/

  printf("%i, %li ms\n", r, totalTime/MEASUREMENTS);

  return 0;
}


