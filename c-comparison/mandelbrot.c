#include <time.h>
#include <stdio.h>
#include <stdlib.h>



int compute(double xc, double yc, double threshold) {
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


void mandelbrot(int* image, int size, double xlo, double ylo, double xhi, double yhi, int threshold) {
  int idx = 0;
  int until = size * size;
  for (idx = 0; idx < until; idx++) {
    int x = idx % size;
    int y = idx / size;
    double xc = xlo + (xhi - xlo) * x / size;
    double yc = ylo + (yhi - ylo) * y / size;
    image[idx] = compute(xc, yc, threshold);
  }
}


int main(int argc, char *argv[]) {
  clock_t begin, end;
  int size, threshold;
  int* image = NULL;
  double xlo, ylo, xhi, yhi;

  sscanf(argv[1], "%d", &size);
  sscanf(argv[2], "%lf", &xlo);
  sscanf(argv[3], "%lf", &ylo);
  sscanf(argv[4], "%lf", &xhi);
  sscanf(argv[5], "%lf", &yhi);
  sscanf(argv[6], "%d", &threshold);

  image = malloc(sizeof(int) * (size * size));

  begin = clock();
  mandelbrot(image, size, xlo, ylo, xhi, yhi, threshold);
  end = clock();
  
  printf("time: %lu\n", end - begin);
  return 1;
}





