//#include <time.h>
#include <cstdio>
#include <cstdlib>
#include <cmath>

#include <sys/time.h>

//#include <iostream>
//#include <vector>
#include <ctime>

#define MESUREMENTS 30


using namespace std;

int* image;

struct Sum {
  /*    int value;
    Sum() : value(0) {}
    Sum( Sum& s, split ) {value = 0;}*/

//   int size;
   int wdt;
   int hgt;
   double xlo;
   double ylo;
   double xhi;
   double yhi;
   int threshold;

  void operator()(int start, int end ) const {
//     return;
      

//      int t_id = std::this_thread::get_id();
//      printf("  working on %d %d size %d\n " , idx, until, until - idx);

      #pragma omp parallel for
      for (int idx = start; idx < end; idx++) {
        volatile int value; 
        int x = idx % wdt;
        int y = idx / wdt;
        double xc = xlo + (xhi - xlo) * x / wdt;
        double yc = ylo + (yhi - ylo) * y / hgt;
        value = compute(xc, yc);
      }
    }

   int compute(const double xc, const double yc) const {
     int i = 0;
     double x = 0.0;
     double y = 0.0;
       while (x * x + y * y < 4 && i < threshold) {
       double xt = x * x - y * y + xc;
       double yt = 2 * x * y + yc;
       x = xt;
       y = yt;
       i += 1;
     }

    return i;
    }

    //void join( Sum& rhs ) {value += rhs.value;}
};

void ParallelSum(double xlo, double ylo, double xhi, double yhi, int wdt, int hgt, int threshold) {
    Sum total;
    total.wdt = wdt;
    total.hgt = hgt;
    total.xlo = xlo;
    total.ylo = ylo;
    total.yhi = yhi;
    total.xhi = xhi;
    total.threshold = threshold;

       total(1,  wdt * hgt);
}


int main(int argc, char *argv[]) {
  time_t start,end;
  int threshold;
  int* image = NULL;
  double xlo, ylo, xhi, yhi;
  int wdt, hgt;
  int gran;
  sscanf(argv[1], "%d", &wdt);
  sscanf(argv[2], "%d", &hgt);
  sscanf(argv[3], "%lf", &xlo);
  sscanf(argv[4], "%lf", &ylo);
  sscanf(argv[5], "%lf", &xhi);
  sscanf(argv[6], "%lf", &yhi);
  sscanf(argv[7], "%d", &threshold);



  
      struct timeval time;
      gettimeofday(&time, NULL); // Start Time
      long totalTime = (time.tv_sec * 1000) + (time.tv_usec / 1000);
      for (int j = 0; j< MESUREMENTS; j++) {
      ParallelSum(xlo, ylo, xhi, yhi, wdt, hgt,  threshold);
    }
      gettimeofday(&time, NULL);  //END-TIME
      totalTime = (((time.tv_sec * 1000) + (time.tv_usec / 1000)) - totalTime);

/*     for(int j = 0; j < i * 10000000; j++) {
       printf("1");
       }*/

      printf("%i, %li\n",0, totalTime/MESUREMENTS);

  

  return 1;
}
