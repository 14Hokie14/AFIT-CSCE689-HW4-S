#ifndef REPLSERVER_H
#define REPLSERVER_H

#include <map>
#include <memory>
#include "QueueMgr.h"
#include "DronePlotDB.h"

/***************************************************************************************
 * ReplServer - class that manages replication between servers. The data is automatically
 *              sent to the _plotdb and the replicate method loops, handling replication
 *              until _shutdown is set to true. The QueueMgr object does the majority of
 *              the communications. This object simply runs management loops and should
 *              do deconfliction of nodes
 *
 ***************************************************************************************/
class ReplServer 
{
public:
   ReplServer(DronePlotDB &plotdb, const char *ip_addr, unsigned short port, int offset,
                              float _time_mult = 1.0, unsigned int verbosity = 1);
   ReplServer(DronePlotDB &plotdb, float _time_mult = 1.0);
   virtual ~ReplServer();

   // Main replication loop, continues until _shutdown is set
   void replicate(const char *ip_addr, unsigned short port);
   void replicate();
  
   // Call this to shutdown the loop 
   void shutdown();

   // An adjusted time that accounts for "time_mult", which speeds up the clock. Any
   // attempts to check "simulator time" should use this function
   time_t getAdjustedTime();

private:

   void addReplDronePlots(std::vector<uint8_t> &data);
   void addSingleDronePlot(std::vector<uint8_t> &data);

   unsigned int queueNewPlots();


   QueueMgr _queue;    

   // Holds our drone plot information
   DronePlotDB &_plotdb;

   bool _shutdown;

   // How fast to run the system clock - 1.0 = normal speed, 2.0 = 2x as fast
   float _time_mult;

   // System clock time of when the server started
   time_t _start_time;

   // When the last replication happened so we can know when to do another one
   time_t _last_repl;

   // How much to spam stdout with server status
   unsigned int _verbosity;

   // Used to bind the server
   std::string _ip_addr;
   unsigned short _port;

   // Don't worry about consistency until the servers have fully run their course. The
   // reasoning behind is that lets say we have 3 nodes and you don't actually get any 
   // data points on the last node unitl the very last tick of the day (session, whatever)
   // so we are just going to make sure the data store at the very end is consistent. 
   // We will make the largest node the true time and change the offsets based on that.  
   void eventual_consistency();

   // Itterate through the database and add unique nodes to the tracker vector
   void get_node_count(std::vector<unsigned int> &tracker);

   // Generate the offsets with the highest node id being the "true" time
   void generate_offsets(std::vector<int> &offsets, std::vector<unsigned int> &tracker);

   // Generate offset auxillary function
   int offset_aux(unsigned int node_id, unsigned int largest_id);

   // loop through the database and correct the timestamps
   void correct_timestamps(std::vector<int> &offsets, std::vector<unsigned int> &tracker);
};


#endif
