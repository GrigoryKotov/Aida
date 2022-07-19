I will explain about the pipeline of my code.


Firstly the Server reads all of the input files and merge them into one file. So that I can understand the whole data size.
After that check how many clients are available at that time.(The clients are already registered)
After checking, Server decides to use 60% of clients as Map Client and rest are Reduce Client.
And then Server sends data to Map Clients. For this I use command prompt 'curl ServerAddress:5000/mapserver'
Then the string datas send to the Map Clients.

And in this time the Map Clients get data from server. And check if this if for Map ro Reduce.
Map clients seperate the string data into words and send back to Server.

When the Server gets all of data from Map clients, then I will send intermediate data to Reduce Clients.
For this I use 'curl ServerAddress:5000/reduceserver'. Then the intermediate data send to Reduce Clients.
At this time Clients check if this is for Map or Reduce.
Reduce clients calculate the total word counts and send back to Server.
Then the server save them into txt file.

I have described simply about my code.
If you want more description or explain, plz notice me about that.
Thanks.

Larry.