from os import listdir
from os.path import join
from flask import Flask
from flask import request
import requests

# Input path
inputpath = 'inputs'

# Read all of the files in input path
filenames = [f for f in listdir('inputs') if f.endswith('.txt')]

mystring = ''
lineno = 0

# Calculate the lines of all files
for fname in filenames:
    with open(join(inputpath, fname)) as infile:
        for line in infile:
            mystring += line + '\n'
            lineno += 1
                

# Store all of the ip addresses of clients(or it can be done with json file or anyother)
cloud_clients = ['192.168.35.31:5001','192.168.35.36:5001','192.168.35.38:5001', '192.168.35.42:5001']

app = Flask(__name__)

# Variables to store the ip addresses of clients for Map and Reduce
alive_clients = []
clients = []
reduce_clients = []


# Map service part
@app.route('/mapserver')
def get_tasks():
    
    line_length = int(lineno/len(clients))
    for i in range(len(clients)):
        
        # Split whole string 
        start = i*line_length
        end = (i+1)*line_length

        if i == len(clients)-1:
            end = lineno

        # Send data to map client
        requests.post('http://' + clients[i] + '/client', headers={'Content-Type': 'application/json'}, json = {'data':mystring[start:end], 'fmt':'map'})        

    return 'Map'


# Reduce service part
@app.route('/reduceserver')
def reduce():

    # Read all of intermediate files in 'intermediate' folder
    filenames = [f for f in listdir('intermediate') if f.endswith('.txt')]
    
    for i in range(len(reduce_clients)):
        s = ''
        for name in filenames:
            name = str(name)
            if name.split('.')[0].split('_')[2] == str(i):
                with open('intermediate/'+name, 'r') as file:
                    # Merge all of the files which has same bucket num
                    s += file.read()
        
        # Send data to reduce client
        requests.post('http://' + reduce_clients[i] + '/client', headers={'Content-Type': 'application/json'}, json = {'data':s, 'fmt':'reduce'})
    return 'Reduce'


# Get the result of reduce from client
@app.route('/getreduce', methods=['POST'])
def get_reduce():

    if request.method == 'POST':

        data = request.get_json()['data']
        text_file = open('out/out-' + str(reduce_clients.index(str(request.remote_addr)+':5001'))+'.txt','a')        
        
        target = str(data)
        target.replace('[','')
        target.replace(']','')
        
        words = target.split(',')
        print(words)

        # Save result to text file
        for word, count in zip(words[0::1], words[0::2]):
            print(word + '\t' + count)
            text_file.write(word + '\t' + count + '\n')
       
        text_file.close()
    return 'Get Reduce'


# Get map result from clients
@app.route('/getmap', methods=['POST'])
def get_mapdata():

    if request.method == 'POST':

        data = request.get_json()
        s = str(data['data'])
        
        words = s.lower().split()
        map_task_id = clients.index(request.remote_addr+':5001')        

        # Save data to text file
        for word in words:            
            wor = word.lower()
            bucket_id = ord(wor[0]) % len(reduce_clients)
            text_file = open('intermediate/mr_' + str(map_task_id) + '_' + str(bucket_id)+'.txt','a')
            text_file.write(wor+'\t'+'1'+'\n')
            text_file.close()                   
     
    return s


if __name__ == '__main__':

    # Checking how many registered clients are connected
    for i in range(len(cloud_clients)):        
        print('index is : ' + str(i) + ' **ip is : ' + cloud_clients[i])    
    
        try:
            if requests.post('http://' + cloud_clients[i] + '/main').status_code == 200:
                alive_clients.append(cloud_clients[i])
        except:
            pass
    
    # Make that 60% of clients are Map and rest are Reduce clients
    point = int(len(alive_clients) * 0.6)    
    clients = alive_clients[:point]
    reduce_clients = alive_clients[point:]
    
    # Run server
    app.run(debug=True, host='192.168.35.36', port=5000)