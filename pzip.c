///////////////////Libraries///////////////////////
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include<string.h>
#include <sys/mman.h> //Library for mmap
#include <pthread.h>
#include <sys/stat.h> //Library for struct stat
#include <assert.h>
#include <sys/sysinfo.h>
#include <unistd.h>
///////////////////////////////////////////////////

//////////////////////NOTES/////////////////////////////
/*void *mmap(void *addr, size_t length, int prot, int flags,
                  int fd, off_t offset);

Struct stat contains:
dev_t     st_dev     Device ID of device containing file. 
ino_t     st_ino     File serial number. 
mode_t    st_mode    Mode of file (see below). 
nlink_t   st_nlink   Number of hard links to the file. 
uid_t     st_uid     User ID of file. 
gid_t     st_gid     Group ID of file. 
[XSI][Option Start]
dev_t     st_rdev    Device ID (if file is character or block special). 
[Option End]
off_t     st_size    For regular files, the file size in bytes. 
                     For symbolic links, the length in bytes of the 
                     pathname contained in the symbolic link. 
*/
//////////////////////////////////////////////////////////

/////////////////GLOBAL VARIABLES////////////////////////
int total_threads; //Total number of threads that will be created for consumer.
int page_size; //Page size = 4096 Bytes
int num_files; //Total number of the files passed as the arguments.
int isComplete=0; //Flag needed to wakeup any sleeping threads at the end of program.
int total_pages; //required for the compressed output
int q_head=0; //Circular queue head.
int q_tail=0;
#define q_capacity 10 //Circular queue current size. We can not have static array 
//for buf which size is given as a variable, so use define. (Stackoverflow)
int q_size=0; //Circular queue capacity.
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER, filelock=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER, fill = PTHREAD_COND_INITIALIZER;
int* pages_per_file;
/////////////////////////////////////////////////////////

/////////////////STRUCTURES///////////////////////////////

//Contains the compressed output
struct output {
	char* data;
	int* count;
	int size;
}*out;

//Contains page specific data of a specific file.
struct buffer {
    char* address; //Mapping address of the file_number file + page_number page
    int file_number; //File Number
    int page_number; //Page number
    int chunk_size; //page size or last paged size
}buf[q_capacity];

////////////////////////////////////////////////////////

/////////////////QUEUE Functions////////////////////////

//buf is the Buffer queue. Queue capacity by default = 10
//Add at q_head index of the circular queue. 
void push(struct buffer b)
{
  buf[q_head] = b; //Enqueue the buffer
  q_head = (q_head + 1) % q_capacity;
  q_size++;
}

//Remove from q_tail index of the circular queue.
struct buffer pop()
{
  struct buffer b = buf[q_tail]; //Dequeue the buffer.
  q_tail = (q_tail + 1) % q_capacity;
  q_size--;
  return b;
}

////////////////////////////////////////////////////////

////////////////////////PRODUCER/////////////////////////
//Reference for MMAP code: http://man7.org/linux/man-pages/man2/mmap.2.html
//Producer function to memory map the files passed as arguments.
void* producer(void *arg){
	//Step 1: Get the file.
	char** filenames = (char **)arg;
	struct stat sb;
	char* map; //mmap address
	int file;
	
	//Step 2: Open the file
	for(int i=0;i<num_files;i++){
		file = open(filenames[i], O_RDONLY);
		int pages_in_file=0; //Calculates the number of pages in the file. Number of pages = Size of file / Page size.
		int last_page_size=0; //Variable required if the file is not page-alligned ie Size of File % Page size !=0
		
		if(file == -1){ //yikes , possible due to file not found?
			printf("Error: File didn't open\n");
			exit(1);
		}
		//Step 3: Get the file info.
		if(fstat(file,&sb) == -1){ //yikes #2.
			close(file);
			printf("Error: Couldn't retrieve file stats\n");
			exit(1);
		}
		//Step 4: Calculate the number of pages and last page size.
		//st_size contains the size offset. 
		pages_in_file=(sb.st_size/page_size);
		//In case file is not page alligned, we'll need to assign an extra page.
		if(sb.st_size%page_size!=0){ 
			pages_in_file+=1;
			last_page_size=sb.st_size%page_size;
		}
		else{ //Page alligned
			last_page_size=page_size;
		}
		total_pages+=pages_in_file;
		pages_per_file[i]=pages_in_file;
		//Compressed output pointer. Re-allocates for every new page.
		if(out==NULL){
			out=malloc(total_pages*sizeof(struct output));
		}
		else{
			struct output* newOut=realloc(out,(sizeof(struct output)*total_pages));
			if(newOut==NULL){
				printf("Error: Problem with reallocating\n");
				exit(1);
			}
			else{
				out=newOut;
			}
		}
		//printf("after alloc out\n");
		//Step 5: Map the entire file.
		map = mmap(0, sb.st_size, PROT_READ, MAP_SHARED, file, 0);
		if (map == MAP_FAILED) { //yikes #3,possibly due to no memory?
			close(file);
			perror("Error mmapping the file\n");
			exit(1);
    	}
    	
    	//Step 6: For all the pages in file, create a Buffer type data with the relevant information for the consumer.
		for(int j=0;j<pages_in_file;j++){
			while(q_size==q_capacity){
				pthread_cond_wait(&empty,&lock); //Call the consumer to start working on the queue.
			}
			struct buffer temp;
			if(j==pages_in_file-1){ //Last page, might not be page-alligned
				temp.chunk_size=last_page_size;
			}
			else{
				temp.chunk_size=page_size;
			}
			temp.address=map;
			temp.page_number=j;
			temp.file_number=i;
			map+=page_size; //Go to next page.
			push(temp);
			pthread_cond_signal(&fill);
		}
		//Step 7: Close the file.
		close(file);
	}
	//Debugging step: Program wasn't ending during some runtimes as consumer threads were waiting for work.
	isComplete=1; //When producer is done mapping.
	pthread_cond_broadcast(&fill); //Wake-up all the sleeping consumer threads.
	//printf("Returning from producer\n");
	return 0;
}
/////////////////////////////////////////////////////////////////////////

///////////////////////////CONSUMER/////////////////////////////////////

//Compresses the buffer object.
struct output RLECompress(struct buffer temp){
    //printf("in compress\n");
	struct output compressed;
	compressed.count=malloc(temp.chunk_size*sizeof(int));
	char* tempString=malloc(temp.chunk_size);
	int countIndex=0;
	//printf("%d\n",temp.chunk_size);
	for(int i=0;i<temp.chunk_size;i++){
		tempString[countIndex]=temp.address[i];
		compressed.count[countIndex]=1;
		while(temp.address[i]==temp.address[i+1] && i+1<temp.chunk_size){
		    //printf("test\n");
			compressed.count[countIndex]++;
			i++;
		}
		countIndex++;
	}
	//printf("loop completed\n");
	compressed.size=countIndex;
	compressed.data=realloc(tempString,countIndex);
	//printf("returning from compress\n");
	return compressed;
}

//Calculates the relative output position for the buffer object.
int calculateOutputPosition(struct buffer temp){
	int position=0;
	//step 1: Find the relative file position of the buffer object.
	for(int i=0;i<temp.file_number;i++){
		position+=pages_per_file[i];
	}
	//Now we're at the index where the particular file begins, we need
	//to find the page relative position
	position+=temp.page_number;
	return position;
}

//Called by consumer threads to start memory compression
//of the files in the queue 'buf'
void *consumer(){
    //printf("in consumer\n");
	do{
		pthread_mutex_lock(&lock);
		while(q_size==0 && isComplete!=1){
			pthread_cond_wait(&fill,&lock); //call the producer to start filling the queue.
		}
		pthread_mutex_unlock(&lock);
		if(isComplete==1 && q_size==0){ //If producer is done mapping and there's nothing left in the queue.
		    //printf("Thread exiting\n");
			pthread_exit(NULL);
		}
		pthread_mutex_lock(&filelock); //Possible race condition while dequeuing file.
		struct buffer temp=pop();
		pthread_mutex_unlock(&filelock);
		//Output position calculation
		int position=calculateOutputPosition(temp);
		out[position]=RLECompress(temp);
	}while((isComplete!=1 && q_size!=0));
	//printf("Returning from consumer\n");
	return NULL;
}

////////////////////////////////////////////////////////////////////////

///////////////////////////Main/////////////////////////////////////////


void printOutput(){
	char* finalOutput=malloc(total_pages*page_size*(sizeof(int)+sizeof(char)));
	char* init=finalOutput; //contains the starting point of finalOutput pointer
	//printf("in printoutput\n");
	for(int i=0;i<total_pages;i++){
	    //printf("in loop1\n");
	    //printf("%c\n",out[i].data[out[i].size-1]);
	    //printf("%c\n",out[i].data[out[i].size-1]);
		if(i<total_pages-1){
			if(out[i].data[out[i].size-1]==out[i+1].data[0]){ //Compare i'th output's last character with the i+1th output's first character
				out[i+1].count[0]+=out[i].count[out[i].size-1];
				out[i].size--;
			}
		}
		for(int j=0;j<out[i].size;j++){
			int num=out[i].count[j];
			char character=out[i].data[j];
			*((int*)finalOutput)=num;
			finalOutput+=sizeof(int);
			*((char*)finalOutput)=character;
			finalOutput+=sizeof(char);
			//printf("%c : %d\n",character,num);
			//fwrite(&num,sizeof(int),1,stdout);
			//fwrite(&character,sizeof(char),1,stdout);
		}
	}
	fwrite(init,finalOutput-init,1,stdout);
}

int main(int argc, char* argv[]){
	//Check if less than two arguments
	if(argc<2){
		printf("pzip: file1 [file2 ...]\n");
		exit(1);
	}

	//Initialize all the global Variables.
	page_size = sysconf(_SC_PAGE_SIZE); //4096 bytes
	num_files=argc-1; //Number of files, needed for producer.
	total_threads=get_nprocs(); //Number of processes consumer threads 
	pages_per_file=malloc(sizeof(int)*num_files); //Pages per file.

	pthread_t pid,cid[total_threads];
	pthread_create(&pid, NULL, producer, argv+1); //Let the mapping begin?
	for (int i = 0; i < total_threads; i++) {
        pthread_create(&cid[i], NULL, consumer, NULL);
    }

    for (int i = 0; i < total_threads; i++) {
        pthread_join(cid[i], NULL);
    }
    pthread_join(pid,NULL);
	printOutput();
	return 0;
}
//////////////////////////////////////////////////////////////////////////
