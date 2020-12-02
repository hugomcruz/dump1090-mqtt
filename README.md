Work in progress. The documentation is not completed yet. (2-Dec-2020)


# DUMP1090 -> MQTT
This software connects to dump1090 to receive ADS-B data and processes it to send to a MQTT Server. 
It has 1 main component:
- publisher

And it has 3 sample subscriber:
- dumper
- store
- tibco-gallery
Each one of these is described below


## publisher
This is the the main component, that connects to the port 30003 on dump1090.
The logic is:
- Receive the data and process it. 
- Reduce the information
- Batch the information by time window (default: 3 seconds)
- Compress the payload - gzip algorithm
- Publish to MQTT. 

The information is batched into a time window, to achieve maximum compression on the payload, to minimize the bandwidth quota on the transmition. This was proved to save substancial amounts of data when running on a Raspberry Pi, connected via a 4G dongle. 




## sample subscribers

### dumper

### store

### tibco-gallery
As of December 2020, I work for TIBCO Software and I am using this software to send data to Analytics Demo Gallery: 
https://www.tibco.com/products/tibco-spotfire/learn/demos/real-time-flight-tracker

Feel free to visit the page and move the map to Ho Chi Minh City, Vietnam; where I have my reveiver setup running this software to feed the gallery. 








