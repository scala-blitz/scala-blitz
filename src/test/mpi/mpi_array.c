/******************************************************************************
* FILE: mpi_array.c
* DESCRIPTION: 
*   MPI Example - Array Assignment - C Version
*   This program demonstrates a simple data decomposition. The master task
*   first initializes an array and then distributes an equal portion that
*   array to the other tasks. After the other tasks receive their portion
*   of the array, they perform an addition operation to each array element.
*   They also maintain a sum for their portion of the array. The master task 
*   does likewise with its portion of the array. As each of the non-master
*   tasks finish, they send their updated portion of the array to the master.
*   An MPI collective communication call is used to collect the sums 
*   maintained by each task.  Finally, the master task displays selected 
*   parts of the final array and the global sum of all array elements. 
*   NOTE: the number of MPI tasks must be evenly disible by 4.
* AUTHOR: Blaise Barney
* LAST REVISED: 04/13/05
****************************************************************************/
#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define DEBUG if(0)

//#define  ARRAYSIZE	16000000
#define  ARRAYSIZE	50000000
#define  MASTER		0
#define MESUREMENTS 5

int  data[ARRAYSIZE];

int main (int argc, char *argv[])
{
int   numtasks, taskid, rc, dest, offset, i, j, tag1,
      tag2, source, chunksize; 
int mysum, sum;
int update(int myoffset, int chunk, int myid);
MPI_Status status;

/***** Initializations *****/
MPI_Init(&argc, &argv);
MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
if (numtasks % 4 != 0) {
   printf("Quitting. Number of MPI tasks must be divisible by 4.\n");
   MPI_Abort(MPI_COMM_WORLD, rc);
   exit(0);
   }
MPI_Comm_rank(MPI_COMM_WORLD,&taskid);

printf ("MPI task %d has started...\n", taskid);
chunksize = (ARRAYSIZE / numtasks);
tag2 = 1;
tag1 = 2;
clock_t end;
clock_t begin;
int l=0;
if(taskid == MASTER) {
  sum = 0;
  for(i=0; i<ARRAYSIZE; i++) {
    data[i] =  i;
    sum = sum + data[i];
    }
  printf("Initialized array sum = %d\n",sum);
  begin = clock();
}

for(l=0; l< MESUREMENTS; l++) {
/***** Master task only ******/
if (taskid == MASTER){

  /* Initialize the array */





  /* Send each task its portion of the array - master keeps 1st part */
  offset = chunksize;
  for (dest=1; dest<numtasks; dest++) {
    MPI_Send(&offset, 1, MPI_INT, dest, tag1, MPI_COMM_WORLD);
    MPI_Send(&data[offset], chunksize, MPI_INT, dest, tag2, MPI_COMM_WORLD);
    DEBUG printf("Sent %d elements to task %d offset= %d\n",chunksize,dest,offset);
    offset = offset + chunksize;
    }

  /* Master does its part of the work */
  offset = 0;
  mysum = update(offset, chunksize, taskid);

  /* Wait to receive results from each task */
  for (i=1; i<numtasks; i++) {
    source = i;
    MPI_Recv(&offset, 1, MPI_INT, source, tag1, MPI_COMM_WORLD, &status);
    MPI_Recv(&data[offset], chunksize, MPI_INT, source, tag2,
      MPI_COMM_WORLD, &status);
    }

  /* Get final sum and print sample results */  
  MPI_Reduce(&mysum, &sum, 1, MPI_INT, MPI_SUM, MASTER, MPI_COMM_WORLD);

 end = clock(); 
 DEBUG printf("Sample results: \n");
  offset = 0;
  for (i=0; i<numtasks; i++) {
    for (j=0; j<5; j++) 
      DEBUG printf("  %d",data[offset+j]);
    DEBUG printf("\n");
    offset = offset + chunksize;
    }

  DEBUG printf("*** Final sum= %d ***\n",sum);

  }  /* end of master section */




/***** Non-master tasks only *****/

if (taskid > MASTER) {

  /* Receive my portion of array from the master task */
  source = MASTER;
  MPI_Recv(&offset, 1, MPI_INT, source, tag1, MPI_COMM_WORLD, &status);
  MPI_Recv(&data[offset], chunksize, MPI_INT, source, tag2, 
    MPI_COMM_WORLD, &status);

  mysum = update(offset, chunksize, taskid);

  /* Send my results back to the master task */
  dest = MASTER;
  MPI_Send(&offset, 1, MPI_INT, dest, tag1, MPI_COMM_WORLD);
  MPI_Send(&data[offset], chunksize, MPI_INT, MASTER, tag2, MPI_COMM_WORLD);

  MPI_Reduce(&mysum, &sum, 1, MPI_INT, MPI_SUM, MASTER, MPI_COMM_WORLD);

  } /* end of non-master */
}

MPI_Finalize();
if(taskid == MASTER) { 
  double elapsed_msecs = (1000*end - 1000*begin) *1.0f / CLOCKS_PER_SEC/MESUREMENTS;
  printf("sample time: %lf\n", elapsed_msecs);
}

}   /* end of main */


int update(int myoffset, int chunk, int myid) {
  int i; 
  int mysum;
  /* Perform addition to each of my array elements and keep my sum */
  mysum = 0;
  for(i=myoffset; i < myoffset + chunk; i++) {
    data[i] = data[i] + i * 1.0;
    mysum = mysum + data[i];
    }
  DEBUG printf("Task %d mysum = %d\n",myid,mysum);
  return(mysum);
  }

