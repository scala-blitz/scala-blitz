#include <cilk/cilk.h>
#include <cilk/reducer_opadd.h> //needs to be included to use the addition reducer 
#include <iostream>

#include <vector>
#include <ctime>
#define N 50000000
#define MESUREMENTS 30

using namespace std;

int main(){
//  std::vector<int> data;
//  int sz = N;
//  for (int i=0;i<sz;++i)
//    data.push_back(i);
  clock_t begin = clock();

  cilk::reducer_opadd<int> sum;  //defining the sum as a reducer with an int value
  cilk_for (int i = 0; i <= 10000; i++)
    sum += i;

  int r = 0;//sum.get_value();
  clock_t end = clock();

  double elapsed_msecs = double(1000*end - 1000*begin) / CLOCKS_PER_SEC/MESUREMENTS;

  std::cout<< r <<' ' << elapsed_msecs<< std::endl;

//  coutprintf("%d\n",sum.get_value()); //notice that sum is now an object
} 
