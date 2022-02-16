# bdp_final_project - Map Reduce for Small Files Using Object Storage
This is our solution for solving the map reduce with small files problem using object storage. 

In the original MapReduce procedure the first step was to separate our files into chunks, each chunk then will get a mapper and the procedure goes on. The problem with small files is that we are losing the parallelism of working on small chunks of a file. If a file is smaller than a chunk or even a little bigger we will need a mapper for each file and the whole process won’t work in parallel.
Our approach is to divide our files into small groups and treat each group as a chunk. Each mapper will be allocated a group and when the mapper function starts it will read all the files in the group and process them together. After that phase the rest of the process will continue without a change.
By using this approach we are getting an efficient map function, since each map function works on a small portion of the data.

We are saving effort by using one client for reading several files and we won’t need a whole new phase for concating the files.
The advantage of this approach is that we will be able to use the mapper functions in a parallel way and achieve efficiency of the process. We also won’t need to create a new session with the object storage for every file read and will use the same session for several files. Another advantage is that we get to keep the original data and the whole procedure after the mapping stage will continue as usual. 
The disadvantage of our approach is that this approach won’t be a good fit for a mixture of small and big files or only for big files. Another disadvantage is that it’s very hard to find a number that will make the file partition optimal. This number is also different for each case and an optimal number for one user won’t be as optimal for a different user. 
