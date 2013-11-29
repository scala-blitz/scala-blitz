#include <time.h>
#include <stdio.h>
#include <stdlib.h>



int op(int e, int size) {
  int until = 1;
  if (e > size * 0.9995) until = 2000000;
  int acc = 1;
  int i = 1;
  while (i < until) {
    acc *= i;
    i += 1;
    //asm("");
  }
  return acc;
}

int rangefoldplus(int limit) {
  int sum = 0;
  int i = 0;
  while (i < limit) {
    sum += op(i, limit);
    i += 1;
    asm("");
  }
  return sum;
}


int main(int argc, char *argv[]) {
  clock_t begin, end;
  int sum;
  int limit;
  int* array = NULL;

  sscanf(argv[1], "%d", &limit);

  begin = clock();
  sum = rangefoldplus(limit);
  end = clock();
  printf("time: %lu\n", end - begin);
  printf("result: %d\n", sum);
  return sum;
}





