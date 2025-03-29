### Question 1 ### 
# It is important to ensure message ordering per monitor => I will implement a topic 'PressData' with 2 partitions; each 
# monitor (acting as a consumer) will be sending its data to its corresponding partition in the topic. 

# Since we have 2 applications that want to re-process the same data each, we cannot have them as customers/clients inside the same consumer group, 
# since a topic's partition can only be read from a single consumer of a given consumer group. 
# Thus, we will create 2 consumer groups; 1 to accommodate each app's scope. 



### Question 2 ###

