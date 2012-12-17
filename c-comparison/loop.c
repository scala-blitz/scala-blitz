#include <time.h>
#include <stdio.h>



int compute(int limit) {
  int sum = 1;
  int i = 1;
  while (i < limit) {
    sum *= i;
    i++;
  }
  return sum;
}


int main(int argc, char *argv[]) {
  clock_t begin, end;
  int sum;
  int limit;
  sscanf(argv[1], "%d", &limit);
  begin = clock();
  sum = compute(limit);
  end = clock();
  printf("time: %lu\n", end - begin);
  printf("result: %d\n", sum);
  return sum;
}





