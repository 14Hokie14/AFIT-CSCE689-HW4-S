#include <iostream>
#include <exception>
#include "ReplServer.h"

const time_t secs_between_repl = 20;
const unsigned int max_servers = 10;

/*********************************************************************************************
 * ReplServer (constructor) - creates our ReplServer. Initializes:
 *
 *    verbosity - passes this value into QueueMgr and local, plus each connection
 *    _time_mult - how fast to run the simulation - 2.0 = 2x faster
 *    ip_addr - which ip address to bind the server to
 *    port - bind the server here
 *
 *********************************************************************************************/
ReplServer::ReplServer(DronePlotDB &plotdb, float time_mult)
                              :_queue(1),
                               _plotdb(plotdb),
                               _shutdown(false), 
                               _time_mult(time_mult),
                               _verbosity(1),
                               _ip_addr("127.0.0.1"),
                               _port(9999)
{
   _start_time = time(NULL);
}

ReplServer::ReplServer(DronePlotDB &plotdb, const char *ip_addr, unsigned short port, int offset, 
                        float time_mult, unsigned int verbosity)
                                 :_queue(verbosity),
                                  _plotdb(plotdb),
                                  _shutdown(false), 
                                  _time_mult(time_mult), 
                                  _verbosity(verbosity),
                                  _ip_addr(ip_addr),
                                  _port(port)

{
   _start_time = time(NULL) + offset;
}

ReplServer::~ReplServer() {

}


/**********************************************************************************************
 * getAdjustedTime - gets the time since the replication server started up in seconds, modified
 *                   by _time_mult to speed up or slow down
 **********************************************************************************************/

time_t ReplServer::getAdjustedTime() {
   return static_cast<time_t>((time(NULL) - _start_time) * _time_mult);
}

/**********************************************************************************************
 * replicate - the main function managing replication activities. Manages the QueueMgr and reads
 *             from the queue, deconflicting entries and populating the DronePlotDB object with
 *             replicated plot points.
 *
 *    Params:  ip_addr - the local IP address to bind the listening socket
 *             port - the port to bind the listening socket
 *             
 *    Throws: socket_error for recoverable errors, runtime_error for unrecoverable types
 **********************************************************************************************/

void ReplServer::replicate(const char *ip_addr, unsigned short port) {
   _ip_addr = ip_addr;
   _port = port;
   replicate();
}

void ReplServer::replicate() {

   // Track when we started the server
   _last_repl = 0;

   // Set up our queue's listening socket
   _queue.bindSvr(_ip_addr.c_str(), _port);
   _queue.listenSvr();

   if (_verbosity >= 2)
      std::cout << "Server bound to " << _ip_addr << ", port: " << _port << " and listening\n";

  
   // Replicate until we get the shutdown signal
   while (!_shutdown) {

      // Check for new connections, process existing connections, and populate the queue as applicable
      _queue.handleQueue();     

      // See if it's time to replicate and, if so, go through the database, identifying new plots
      // that have not been replicated yet and adding them to the queue for replication
      if (getAdjustedTime() - _last_repl > secs_between_repl) {

         queueNewPlots();
         _last_repl = getAdjustedTime();
      }
      
      // Check the queue for updates and pop them until the queue is empty. The pop command only returns
      // incoming replication information--outgoing replication in the queue gets turned into a TCPConn
      // object and automatically removed from the queue by pop
      std::string sid;
      std::vector<uint8_t> data;
      while (_queue.pop(sid, data)) {

         // Incoming replication--add it to this server's local database
         addReplDronePlots(data);         
      }       

      usleep(1000);
   }

   // Don't forget the eventual consistency call!
   eventual_consistency();   
}

/**********************************************************************************************
 * queueNewPlots - looks at the database and grabs the new plots, marshalling them and
 *                 sending them to the queue manager
 *
 *    Returns: number of new plots sent to the QueueMgr
 *
 *    Throws: socket_error for recoverable errors, runtime_error for unrecoverable types
 **********************************************************************************************/

unsigned int ReplServer::queueNewPlots() {
   std::vector<uint8_t> marshall_data;
   unsigned int count = 0;

   if (_verbosity >= 3)
      std::cout << "Replicating plots.\n";

   // Loop through the drone plots, looking for new ones
   std::list<DronePlot>::iterator dpit = _plotdb.begin();
   for ( ; dpit != _plotdb.end(); dpit++) {

      // If this is a new one, marshall it and clear the flag
      if (dpit->isFlagSet(DBFLAG_NEW)) {
         
         dpit->serialize(marshall_data);
         dpit->clrFlags(DBFLAG_NEW);

         count++;
      }
      if (marshall_data.size() % DronePlot::getDataSize() != 0)
         throw std::runtime_error("Issue with marshalling!");

   }
  
   if (count == 0) {
      if (_verbosity >= 3)
         std::cout << "No new plots found to replicate.\n";

      return 0;
   }
 
   // Add the count onto the front
   if (_verbosity >= 3)
      std::cout << "Adding in count: " << count << "\n";

   uint8_t *ctptr_begin = (uint8_t *) &count;
   marshall_data.insert(marshall_data.begin(), ctptr_begin, ctptr_begin+sizeof(unsigned int));

   // Send to the queue manager
   if (marshall_data.size() > 0) {
      _queue.sendToAll(marshall_data);
   }

   if (_verbosity >= 2) 
      std::cout << "Queued up " << count << " plots to be replicated.\n";

   return count;
}

/**********************************************************************************************
 * addReplDronePlots - Adds drone plots to the database from data that was replicated in. 
 *                     Deconflicts issues between plot points.
 * 
 * Params:  data - should start with the number of data points in a 32 bit unsigned integer, 
 *                 then a series of drone plot points
 *
 **********************************************************************************************/

void ReplServer::addReplDronePlots(std::vector<uint8_t> &data) {
   if (data.size() < 4) {
      throw std::runtime_error("Not enough data passed into addReplDronePlots");
   }

   if ((data.size() - 4) % DronePlot::getDataSize() != 0) {
      throw std::runtime_error("Data passed into addReplDronePlots was not the right multiple of DronePlot size");
   }

   // Get the number of plot points
   unsigned int *numptr = (unsigned int *) data.data();
   unsigned int count = *numptr;

   // Store sub-vectors for efficiency
   std::vector<uint8_t> plot;
   auto dptr = data.begin() + sizeof(unsigned int);

   for (unsigned int i=0; i<count; i++) {
      plot.clear();
      plot.assign(dptr, dptr + DronePlot::getDataSize());
      addSingleDronePlot(plot);
      dptr += DronePlot::getDataSize();      
   }
   if (_verbosity >= 2)
      std::cout << "Replicated in " << count << " plots\n";   
}


/**********************************************************************************************
 * addSingleDronePlot - Takes in binary serialized drone data and adds it to the database. 
 *
 **********************************************************************************************/

void ReplServer::addSingleDronePlot(std::vector<uint8_t> &data) {
   DronePlot tmp_plot;

   tmp_plot.deserialize(data);

   _plotdb.addPlot(tmp_plot.drone_id, tmp_plot.node_id, tmp_plot.timestamp, tmp_plot.latitude,
                                                         tmp_plot.longitude);
}


void ReplServer::shutdown() {
   _shutdown = true;
}

/**
 * Call this at the end of replicate.  This will remove duplicates and 
 * fix all of the offsets, making them relative to the highest node id.  
 **/ 
void ReplServer::eventual_consistency(){
   std::cout << "calling eventual_consistency" << std::endl;
   // Sort the list at the begining to make the logic easier:
   _plotdb.sortByTime();

   // First figure out how many nodes we have
   std::vector<unsigned int> tracker;
   std::cout << "get_node_count" << std::endl;
   get_node_count(tracker); // tracker now contains all the node ids
   std::sort(tracker.begin(), tracker.end()); // Arrange values in ascending order

   // Now we will generate the offsets and delete the duplicates
   std::vector<int> offsets; // vector to hold the offsets
   std::cout << "generate_offsets" << std::endl;
   generate_offsets(offsets, tracker);

   // Now we can go through and fix the time stamps
   std::cout << "correct_timestamps" << std::endl;
   correct_timestamps(offsets, tracker);
}

// Itterate through the database and add unique nodes to the tracker vector
void ReplServer::get_node_count(std::vector<unsigned int> &tracker){
   auto dbitr = _plotdb.begin(); // Iterator set up

   // Now we are going to loop through every node in the DB and check to see if it 
   // is in tracker. If it is not add it.
   for ( ; dbitr != _plotdb.end(); dbitr++) {
      if (!std::count(tracker.begin(), tracker.end(), dbitr->node_id)){
         tracker.push_back(dbitr->node_id);
      } 
   }
}

// Generate the offsets with the highest node id being the "true" time
void ReplServer::generate_offsets(std::vector<int> &offsets, std::vector<unsigned int> &tracker){
   // Get the value of the largest node id
   unsigned int largest_id = tracker[tracker.size()-1];
   
   // This loop will happen once for each node id in tracker
   for(unsigned int node_id:tracker){
      // If we are on the last node just break
      if(node_id == largest_id){
         break; // we are done
      }
      // Add the offset to offsets, offset_aux also deletes the duplicate node
      offsets.push_back(offset_aux(node_id, largest_id));
   }
   
}

// Auxillary fuction to unclutter the generate offsets function
int ReplServer::offset_aux(unsigned int input_id, unsigned int largest_id){
   std::cout << "calling offset_aux" << std::endl;
   int offset = 0; 

   
   auto dbitr = _plotdb.begin(); 
   auto dbitr2 = _plotdb.begin();
   // Iterate over the plotdb 
   for ( ; dbitr != _plotdb.end(); dbitr++) {
      // if this is the node we are looking to set up the offset for
      if(dbitr->node_id == input_id){
         // now we are going to loop through the DB starting where we currently are
         dbitr2 = dbitr; 
         dbitr2++; // point it to the next plot
         for ( ; dbitr2 != _plotdb.end(); dbitr2++) {
            if(dbitr2->latitude == dbitr->latitude && dbitr2->longitude == dbitr->longitude && dbitr2->node_id == largest_id){
               // at this point dbitr2 is pointing at a node that is a dubplicate of the node dbitr is pointing at
               // but with the node id of largest_id
               offset = dbitr2->timestamp - dbitr->timestamp;
               // also since we have an iterator at a duplicate we can just call erase now too!
               _plotdb.erase(dbitr2);
               return offset;
            }

         }
      }
   }
   return offset; 
}

// Uses the offsets and tracker vectors to iterate through the DB and correct the timestamps
void ReplServer::correct_timestamps(std::vector<int> &offsets, std::vector<unsigned int> &tracker){
   // Get the value of the largest node id
   unsigned int largest_id = tracker[tracker.size()-1];

   // Iterate over the plotdb 
   int i = 0; 
   auto dbitr = _plotdb.begin(); 
   for ( ; dbitr != _plotdb.end(); dbitr++) {
      if(dbitr->node_id != largest_id){
         // Get the index of the node in tracker
         i = 0; 
         for(unsigned int n:tracker){
            if(dbitr->node_id == n){
               break;
            }
            i++;
         }
         dbitr->timestamp = dbitr->timestamp + offsets[i];
      }
   }
}
