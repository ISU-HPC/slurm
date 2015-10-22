#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/resource.h>


int main (argc, argv)
     int argc;
     char *argv[];
{
  printf( "Hello world");

  int i = 0;
  for (i = 0; i < 50; i++) {

    printf("this is iteration %d\n", i);
    sleep(1);
  }

  printf( "Goodbye world");

  return 0;
}
