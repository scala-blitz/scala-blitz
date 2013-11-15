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
#define N 3000000
#define MESUREMENTS 10

using namespace tbb;

struct Sum {
    int value;
    Sum() : value(0) {}
    Sum( Sum& s, split ) {value = 0;}
  int consumeTime(int value) {
    int acc = 1;
    int until = 1;
    if(value > N* 0.9995) until = 2000000;
      
    for(int i = 0; i< until; i++){
      asm("");
      acc = (acc * i) * 3;
      asm("");
      // std::cout<<acc<<std::endl;
    }
    return acc;
  }
    void operator()( const blocked_range<int>& r ) {
        int temp = value;
	int init = r.begin();
	int end = r.end();
	//		if(end<init) std::printf("AAAAAAAAAAAAworking on %d %d size %d. pos %lf\n", init , end ,  end-init, init * 1.0/N);
	//		else std::printf("working on %d %d size %d. pos %lf\n", init , end ,  end-init, init * 1.0/N);
	
        for( int a=r.begin(); a!=r.end(); ++a ) {

	    temp += consumeTime(a);
	    //	    std::cout<<std::endl;

        }
        value = temp;
    }
    void join( Sum& rhs ) {value += rhs.value;}

};



int ParallelSum( size_t n ) {
    Sum total;
    parallel_reduce( blocked_range<int>( 0, n ), 
                     total );
    //total( blocked_range<int>( 0, n ));
    return total.value;
}

int main(int,char**) {

  int sz = N;
  int r;
        struct timeval time;
      gettimeofday(&time, NULL); // Start Time
      long totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000);

  for(int i = 0 ; i< MESUREMENTS; i++) {
  r += ParallelSum(sz);
  std::printf("%i\n", i);
  }

  gettimeofday(&time, NULL);  //END-TIME
  totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000)) - totalTime);

/*     for(int j = 0; j < i * 10000000; j++) {
       printf("1");
       }*/

  printf("%i, %li\n",0, totalTime/MESUREMENTS);

  return 0;
}
