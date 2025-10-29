/*
 * notes to self:
 *    -change n in main to either 32 or 131072 when running
 *    -
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <time.h>

// function to generate a random array that will be sorted
void generate_random_array(int *arr, int n, int max_val) {
    for (int i = 0; i < n; ++i)
        arr[i] = rand() % max_val;
}
// function to check the sorting is actually happening without printing the whole array
// I consulted AI for this idea so I could check the sorting but not have to print the whole 131072 element array
void print_first_last(int *arr, int n, int show_count) {
    if (n <= 0) return;
    int sc;
    if (show_count < n)
        sc = show_count;
    else
        sc = n;
    printf("first %d: ", sc);
    for (int i = 0; i < sc; ++i) printf("%d ", arr[i]);
    printf("\nlast %d: ", sc);
    for (int i = n - sc; i < n; ++i) printf("%d ", arr[i]);
    printf("\n");
}

// function to help with integer comparison. It gets used with qsort function
// if x>y result is 1, if x<y the result is -1, if x==y the result is 0
int compare_ints(const void *a, const void *b) {
    int x = *(int *)a;
    int y = *(int *)b;
    return (x > y) - (x < y);
}

// part 1 parallel sorting info

// Thread structure it has a pointer for shared data array, start index, and end index
typedef struct {
    int *data;
    int start;
    int end;
} ThreadArg;

// sorting a thread with qsort. the arguments for qsort are pointer to start of array, number of elements in array
// size in bytes of each element in the array, and a pointer to comparison function. this is the compare_ints function from above
void *thread_sort(void *arg) {
    ThreadArg *a = (ThreadArg *)arg;
    // I consulted AI to help me with these arguments. I put them in wrong at first.
    qsort(a->data + a->start, a->end - a->start, sizeof(int), compare_ints);
    return NULL;
}

//function to merge all the arrays back into one big array. It computes the elements, makes a temp array for storing,
// computes the offsets, merges the elements, copies back to the original array, and then frees memory
void merge_arrays(int *arr, int *chunk_sizes, int num_chunks) {
    // calculate total elements
    int total = 0;
    for (int i = 0; i < num_chunks; ++i)
        total += chunk_sizes[i];

    // creating temporary array to put elements into
    int *temp = malloc(total * sizeof(int));
    int *idx = calloc(num_chunks, sizeof(int));
    int *offset = malloc(num_chunks * sizeof(int));

    // figuring out where the offsets for each chunk should be
    // AI helped me clear up the logic for this offset part
    offset[0] = 0;
    for (int i = 1; i < num_chunks; ++i)
        offset[i] = offset[i - 1] + chunk_sizes[i - 1];

    // merging loop. It looks through the chunks to find the smallest element then it copies to the temp array,
    // then increments the index for the chunk the number was pulled from. repeats until everything is in temp array
    for (int k = 0; k < total; ++k) {
        int min_val = 2147483647; // start with it initialized to the largest int possible
        int min_i = -1;
        for (int i = 0; i < num_chunks; ++i) {
            if (idx[i] < chunk_sizes[i]) {
                int val = arr[offset[i] + idx[i]];
                if (val < min_val) {
                    min_val = val;
                    min_i = i;
                }
            }
        }
        temp[k] = min_val;
        idx[min_i]++;
    }

    // put the ordered integers into the original array from temp array
    for (int i = 0; i < total; ++i)
        arr[i] = temp[i];

    // free the memory from this temporary stuff
    free(temp);
    free(idx);
    free(offset);
}

// Threads- splits array into chunks, starts one thread per chunk, waits for threads to finish,
// merges the sorted arrays back into a single array, measures clock time and prints in seconds
void run_sort_threads(int *arr, int n, int num_threads) {
    pthread_t threads[num_threads];
    ThreadArg args[num_threads];
    int chunk = n / num_threads;
    int chunk_sizes[num_threads];

    clock_t start = clock();

    for (int i = 0; i < num_threads; ++i) {
        args[i].data = arr;
        args[i].start = i * chunk;
        if (i == num_threads - 1)
            args[i].end = n;
        else
            args[i].end = (i + 1) * chunk;
        chunk_sizes[i] = args[i].end - args[i].start;
        pthread_create(&threads[i], NULL, thread_sort, &args[i]);
    }

    for (int i = 0; i < num_threads; ++i)
        pthread_join(threads[i], NULL);

    merge_arrays(arr, chunk_sizes, num_threads);

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    printf("[Threads=%d] Sorting done in %.5f seconds\n", num_threads, elapsed);
}

// Process version of sorting chunks of array with different number of workers
void run_sort_processes(int *arr, int n, int num_process) {
    int chunk = n / num_process;
    int chunk_sizes[num_process];

    // creates shared memory space not tied to a file that can read and write and is shared between processes
    // I consulted AI for these arguments because I wasn't sure which arguments needed to be included
    int *shared = mmap(NULL, n * sizeof(int),
                       PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    for (int i = 0; i < n; ++i)
        shared[i] = arr[i];

    clock_t start = clock();

    // for number of processes create children to do work
    for (int i = 0; i < num_process; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            int start_i = i * chunk;
            int end_i;
            if (i == num_process - 1) {
                end_i = n;
            } else {
                end_i = (i + 1) * chunk;
            }
            // sorting each chunk
            qsort(shared + start_i, end_i - start_i, sizeof(int), compare_ints);
            exit(0);
        }
        if (i == num_process - 1) {
            chunk_sizes[i] = n - (i * chunk);
        } else {
            chunk_sizes[i] = chunk;
        }
    }
    // wait for processes to be done
    for (int i = 0; i < num_process; ++i)
        wait(NULL);

    merge_arrays(shared, chunk_sizes, num_process);


    for (int i = 0; i < n; ++i)
        arr[i] = shared[i];
    // clock time includes the overhead of the parent copying the array back
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    // freeing shared memory space
    munmap(shared, n * sizeof(int));

    printf("[Processes=%d] Sorting done in %.5f seconds\n", num_process, elapsed);
}

// part 2 Max Value

// thread version
typedef struct {
    int *data;
    int start;
    int end;
    int *shared_max;
    sem_t *sem;
} MaxArg;

void *thread_find_max(void *arg) {
    MaxArg *a = (MaxArg *)arg;
    //thread finding its local max value
    int local_max = a->data[a->start];
    for (int i = a->start + 1; i < a->end; ++i)
        if (a->data[i] > local_max)
            local_max = a->data[i];

    //semaphore waits so only one thread at a time can change the shared max value (entering critical section)
    sem_wait(a->sem);
    if (local_max > *a->shared_max)
        *a->shared_max = local_max;
    //(exiting critical section)
    sem_post(a->sem);

    return NULL;
}
// threads actually finding the max and putting it in shared max and printing the value. semaphore
// protecting shared max value
void run_max_threads(int *arr, int n, int num_threads) {
    pthread_t threads[num_threads];
    MaxArg args[num_threads];
    int chunk = n / num_threads;
    int shared_max = arr[0];
    sem_t sem;
    sem_init(&sem, 0, 1);

    clock_t start = clock();

    for (int i = 0; i < num_threads; ++i) {
        args[i].data = arr;
        args[i].start = i * chunk;
        if (i == num_threads - 1) {
            args[i].end = n;
        }
        else {
            args[i].end = (i + 1) * chunk;
        }
        args[i].shared_max = &shared_max;
        args[i].sem = &sem;
        pthread_create(&threads[i], NULL, thread_find_max, &args[i]);
    }

    for (int i = 0; i < num_threads; ++i)
        pthread_join(threads[i], NULL);

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    sem_destroy(&sem);

    printf("[Threads=%d] Max = %d (%.5f sec)\n", num_threads, shared_max, elapsed);
}

// Process version Max value
void run_max_processes(int *arr, int n, int num_process) {
    int chunk = n / num_process;
    // creates shared max most important part is the size is only one integer
    int *shared_max = mmap(NULL, sizeof(int),
                           PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    //shared memory semaphore
    sem_t *sem = mmap(NULL, sizeof(sem_t),
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    *shared_max = arr[0];
    sem_init(sem, 1, 1);

    clock_t start = clock();

    // creates child processes, gets the start and end, then loops through to find local max
    for (int i = 0; i < num_process; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            int start_i = i * chunk;
            int end_i;
            if (i == num_process - 1) {
                end_i = n;
            }
            else {
                end_i = (i + 1) * chunk;
            }
            int local_max = arr[start_i];
            for (int j = start_i + 1; j < end_i; ++j)
                if (arr[j] > local_max)
                    local_max = arr[j];
            //going into critical section changing shared max if local max is larger
            sem_wait(sem);
            if (local_max > *shared_max)
                *shared_max = local_max;
            //exiting critical section
            sem_post(sem);
            exit(0);
        }
    }

    //waiting for child processes to finish
    for (int i = 0; i < num_process; ++i)
        wait(NULL);

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    printf("[Processes=%d] Max = %d (%.5f sec)\n", num_process, *shared_max, elapsed);

    // destroy semaphore and free up other space
    sem_destroy(sem);
    munmap(shared_max, sizeof(int));
    munmap(sem, sizeof(sem_t));
}

// main

int main() {
    //random numbers will be different every time
    srand(time(NULL));

    //I'm commenting out 131072. to get the bigger number just get rid of 32;//
    //<<<<<<<<THIS LINE DELETE 32; AND UNCOMMENT 131072 TO RUN BIGGER ARRAY>>>>>>>>>>>
    int n = 32;//131072;
    int *arr = malloc(n * sizeof(int));
    //making the array of numbers again I'm commenting out the info for the bigger one just delete 100); for bigger array
    //<<<<<<<<<<THIS LINE DELETE 100); and uncomment 1000000 to run bigger array>>>>>>>>>>>> 
    generate_random_array(arr, n, 100);//1000000);


    printf("\n Part 1: Parallel Sorting (MapReduce Style) \n");

    //loo pfor threads running goes through each senario with each number of workers 1, 2, 4, 8
    for (int workers = 1; workers <= 8; workers *= 2) {
        int *copy = malloc(n * sizeof(int));
        for (int i = 0; i < n; ++i) copy[i] = arr[i];
        run_sort_threads(copy, n, workers);
        print_first_last(copy, n, 5);
        free(copy);
    }

    // loop for the processes to run again it goes through each number of workers 1, 2, 4, 8
    for (int workers = 1; workers <= 8; workers *= 2) {
        int *copy = malloc(n * sizeof(int));
        for (int i = 0; i < n; ++i) copy[i] = arr[i];
        run_sort_processes(copy, n, workers);
        print_first_last(copy, n, 5);
        free(copy);
    }

    // max value loops through threads first then through proceses
    printf("\n Part 2: Max-Value Aggregation with Constrained Shared Memory \n");
    for (int workers = 1; workers <= 8; workers *= 2)
        run_max_threads(arr, n, workers);

    for (int workers = 1; workers <= 8; workers *= 2)
        run_max_processes(arr, n, workers);

    free(arr);
    return 0;
}
