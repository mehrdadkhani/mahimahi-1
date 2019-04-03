#include <sys/file.h>
#include <chrono>
#include <cmath>
#include "pie_packet_queue.hh"
#include "timestamp.hh"
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <zmq.hpp>
#include <ipc_msg.pb.h>

using namespace std;

#define DQ_COUNT_INVALID   (uint32_t)-1

double * _drop_prob = NULL;
double rl_drop_prob = 0.0;
unsigned int _size_bytes_queue = 0;
uint32_t  _current_qdelay = 0;
uint32_t  _current_qdelay_perpacket = 0;
uint64_t dq_counter = 0;
uint64_t eq_counter = 0;
uint64_t dq_bytes = 0;
uint64_t qdelay_total = 0;

int state_rl_enable = 0;



//Update Drop Rate through socket 
void* UpdateDropRateSocket_thread(void* context)
{
	int socketfd = 0;
	int clientfd = 0;
	socklen_t clilen;
	char buffer[1024];

	struct sockaddr_in serv_addr, cli_addr;

	

	socketfd = socket(AF_INET, SOCK_STREAM, 0);

	bzero((char*) &serv_addr, sizeof(serv_addr));

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(4999);
	
	int ret_bind = bind(socketfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));

	int ret_listen = listen(socketfd, 5);
	clilen = sizeof(cli_addr);
	printf("Start to listen %d %d %d\n", socketfd, ret_bind, ret_listen);
	while(true)
	{
		clientfd = accept(socketfd, (struct sockaddr*) &cli_addr, &clilen);
		if(clientfd < 0)
		{
			printf("ERROR on ACCEPT\n");
			return NULL;
		}	
		printf("Accept successfully\n");
		
		while(true)
		{		
			bzero(buffer,1024);
			int n = read(clientfd, buffer, 1023);
			if(n <= 0)
			{
				printf("Client Disappeared... (read) \n");
				close(clientfd);
				break;
			}

			//printf("Read %s\n", buffer);
		
			if(buffer[0] == 'W')
			{
				int a = 0;
				int b = 0;
				int c = 0;
				sscanf(buffer, "W %d %d %d %lf", &a, &b, &c, &rl_drop_prob);
				
				state_rl_enable = b;
			}
			
			if(buffer[0] == 'R')
			{
				//sprintf(buffer, "%lu 0 0 0  0 %lu %lu %u %f 0 0\n", eq_counter, dq_bytes, dq_counter, _current_qdelay, *_drop_prob );
				sprintf(buffer, "%lu 0 0 0  0 %lu %lu %lu %f 0 0\n", eq_counter, dq_bytes, dq_counter, qdelay_total, *_drop_prob );
				int ret = write(clientfd,buffer,strlen(buffer));
				if(ret <= 0)
				{
					printf("Client Disappeared... (write) \n");
					close(clientfd);
					break;
				}
			}

		

		}

	}
return context;
}


//// usign named pipe
//void* UpdateDropRate_thread(void* context)
//{
//  
//  char buffer[1024];
//  int ret;
//  
//  while(true)
//  {
//    printf("waiting mahimahi_pipe in pie\n");
//    int fd1 = open("mahimahi_pipe1",O_RDONLY);
//    ret = read(fd1, buffer, 128);
//    close(fd1);
//
//    //if (ret <=0) {
//    //  continue;
//    //}
//    printf("%d Read %s\n",ret,  buffer);
//  
//    if(buffer[0] == 'W')
//    {
//      int a = 0;
//      int b = 0;
//      int c = 0;
//      sscanf(buffer, "W %d %d %d %lf", &a, &b, &c, &rl_drop_prob);
//      
//      state_rl_enable = b;
//    }
//    
//    if(buffer[0] == 'R')
//    {
//      //sprintf(buffer, "%lu 0 0 0  0 %lu %lu %u %f 0 0\n", eq_counter, dq_bytes, dq_counter, _current_qdelay, *_drop_prob );
//      sprintf(buffer, "%lu 0 0 0  0 %lu %lu %lu %f 0 0\n", eq_counter, dq_bytes, dq_counter, qdelay_total, *_drop_prob );
//      
//      fd1 = open("mahimahi_pipe2",O_WRONLY);
//      ret = write(fd1, buffer, strlen(buffer)+1);
//      close(fd1);
//
//    }
//
//    
//
//    
//
//  }
//return context;
//}

// update drop rate using ipc
void* UpdateDropRate_thread(void* context)
{
  printf("establishing ipc socket on mahimahi\n");
  zmq::context_t context2 (1);
  zmq::socket_t socket (context2, ZMQ_REQ);  
  socket.connect("ipc:///tmp/aqm_cpp_python_ipc");

  rl::IPCMessage request;
  rl::IPCReply reply;
  std::string data;
  zmq::message_t msg;

  while(true)
  {
    request.set_msg("get_prob");
    request.set_eqc(eq_counter);
    request.set_eqb(dq_bytes);
    request.set_dqc(dq_counter);
    request.set_qdelay(qdelay_total);
    request.set_current_prob(*_drop_prob);
    request.SerializeToString(&data);
    socket.send (&data[0],data.size());
    socket.recv (&msg);
    std::string sdata(static_cast<char*>(msg.data()), msg.size());
    reply.ParseFromString(sdata);
    if (strcmp(&reply.msg()[0],"set_prob")==0)
        rl_drop_prob = (double)reply.prob(); 

  }
return context;
}




PIEPacketQueue::PIEPacketQueue( const string & args )
  : DroppingPacketQueue(args),
    qdelay_ref_ ( get_arg( args, "qdelay_ref" ) ),
    max_burst_ ( get_arg( args, "max_burst" ) ),
    alpha_ ( 0.125 ),
    beta_ ( 1.25 ),
    t_update_ ( 20 ),
    dq_threshold_ ( 16384 ),
    drop_prob_ ( 0.0 ),
    burst_allowance_ ( 0 ),
    qdelay_old_ ( 0 ),
    current_qdelay_ ( 0 ),
    dq_count_ ( DQ_COUNT_INVALID ),
    dq_tstamp_ ( 0 ),
    avg_dq_rate_ ( 0 ),
    uniform_generator_ ( 0.0, 1.0 ),
    prng_( random_device()() ),
    last_update_( timestamp() ),
		NN_t( 0 ),
		DP_t( 0 )
{
  if ( qdelay_ref_ == 0 || max_burst_ == 0 ) {
    throw runtime_error( "PIE AQM queue must have qdelay_ref and max_burst parameters" );
  }

	
}

void PIEPacketQueue::enqueue( QueuedPacket && p )
{
	static int counter = 0;
	eq_counter ++;
	counter++;
	if(counter == 2)
	{
		pthread_create(&(this->DP_t),NULL,&UpdateDropRate_thread,NULL);
		printf("Create Pthread!\n");
	}
  calculate_drop_prob();
	
  _drop_prob = &(this->drop_prob_);
	//_current_qdelay = &(this->current_qdelay_);
	if ( this->avg_dq_rate_ > 0 ) 
      _current_qdelay = size_bytes() / this->avg_dq_rate_;
    else
      _current_qdelay = 0;


	_size_bytes_queue = size_bytes();

  //printf("%u\n",size_bytes());

  if ( ! good_with( size_bytes() + p.contents.size(),
		    size_packets() + 1 ) ) {
    // Internal queue is full. Packet has to be dropped.
    return;
  } 

  if (!drop_early() ) {
    //This is the negation of the pseudo code in the IETF draft.
    //It is used to enqueue rather than drop the packet
    //All other packets are dropped
    accept( std::move( p ) );
  }

  assert( good() );
}

//returns true if packet should be dropped.
bool PIEPacketQueue::drop_early ()
{

  // TODO: comment out for now so that the python side can set drop rate
  // if(state_rl_enable == 0)
  // {  
  //   if ( burst_allowance_ > 0 ) {
  //     return false;
  //   }

  //   if ( qdelay_old_ < qdelay_ref_/2 && drop_prob_ < 0.2) {
  //   //if ( qdelay_old_ < qdelay_ref_/2 && rl_drop_prob < 0.2) {
  //     return false;        
  //   }

  //   if ( size_bytes() < (2 * PACKET_SIZE) ) {
  //     return false;
  //   }
  // }
  



  double random = uniform_generator_(prng_);

  //if(state_rl_enable == 1)
  {
    drop_prob_ = rl_drop_prob;
  }

  if ( random < drop_prob_ ) {
  //if ( random < rl_drop_prob ) {
    return true;
  }
  else
    return false;
}

QueuedPacket PIEPacketQueue::dequeue( void )
{
  QueuedPacket ret = std::move( DroppingPacketQueue::dequeue () );
  uint32_t now = timestamp();

  _current_qdelay_perpacket = now - ret.arrival_time;

  dq_counter ++;
  dq_bytes += ret.contents.size();
  qdelay_total += _current_qdelay_perpacket;
  if ( size_bytes() >= dq_threshold_ && dq_count_ == DQ_COUNT_INVALID ) {
    dq_tstamp_ = now;
    dq_count_ = 0;
  }

  if ( dq_count_ != DQ_COUNT_INVALID ) {
    dq_count_ += ret.contents.size();

    if ( dq_count_ > dq_threshold_ ) {
      uint32_t dtime = now - dq_tstamp_;

      if ( dtime > 0 ) {
	uint32_t rate_sample = dq_count_ / dtime;
	if ( avg_dq_rate_ == 0 ) 
	  avg_dq_rate_ = rate_sample;
	else
	  avg_dq_rate_ = ( avg_dq_rate_ - (avg_dq_rate_ >> 3 )) +
		     (rate_sample >> 3);
                
	if ( size_bytes() < dq_threshold_ ) {
	  dq_count_ = DQ_COUNT_INVALID;
	}
	else {
	  dq_count_ = 0;
	  dq_tstamp_ = now;
	} 

	if ( burst_allowance_ > 0 ) {
	  if ( burst_allowance_ > dtime )
	    burst_allowance_ -= dtime;
	  else
	    burst_allowance_ = 0;
	}
      }
    }
  }

  calculate_drop_prob();

  return ret;
}

void PIEPacketQueue::calculate_drop_prob( void )
{
  uint64_t now = timestamp();
	
  //We can't have a fork inside the mahimahi shell so we simulate
  //the periodic drop probability calculation here by repeating it for the
  //number of periods missed since the last update. 
  //In the interval [last_update_, now] no change occured in queue occupancy 
  //so when this value is used (at enqueue) it will be identical
  //to that of a timer-based drop probability calculation.
  while (now - last_update_ > t_update_) {
    bool update_prob = true;
    qdelay_old_ = current_qdelay_;

    if ( avg_dq_rate_ > 0 ) 
      current_qdelay_ = size_bytes() / avg_dq_rate_;
    else
      current_qdelay_ = 0;

    if ( current_qdelay_ == 0 && size_bytes() != 0 ) {
      update_prob = false;
    }

    double p = (alpha_ * (int)(current_qdelay_ - qdelay_ref_) ) +
      ( beta_ * (int)(current_qdelay_ - qdelay_old_) );

    if ( drop_prob_ < 0.01 ) {
      p /= 128;
    } else if ( drop_prob_ < 0.1 ) {
      p /= 32;
    } else  {
      p /= 16;
    } 

    drop_prob_ += p;

    if ( drop_prob_ < 0 ) {
      drop_prob_ = 0;
    }
    else if ( drop_prob_ > 1 ) {
      drop_prob_ = 1;
      update_prob = false;
    }

        
    if ( current_qdelay_ == 0 && qdelay_old_==0 && update_prob) {
      drop_prob_ *= 0.98;
    }
        
    burst_allowance_ = max( 0, (int) burst_allowance_ -  (int)t_update_ );
    last_update_ += t_update_;

    if ( ( drop_prob_ == 0 )
	 && ( current_qdelay_ < qdelay_ref_/2 ) 
	 && ( qdelay_old_ < qdelay_ref_/2 ) 
	 && ( avg_dq_rate_ > 0 ) ) {
      dq_count_ = DQ_COUNT_INVALID;
      avg_dq_rate_ = 0;
      burst_allowance_ = max_burst_;
    }

  }
}
