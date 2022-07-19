from flask import Flask
from flask import request
import requests
import re

app = Flask(__name__)


@app.route('/main', methods=['POST'])
def show_status():

    return "Client"


# Get and do map process with the data from server
@app.route('/client', methods=['POST'])
def get_tasks():

    if request.method == 'POST':

        data = request.get_json()
        # Check if this client is selected as Map Client
        if data['fmt']=='map':
        
            s = data['data']

            # Remove special characters 
            words = re.sub("[^A-Z a-z']","",s)
                        
            words = words.split()
            send = ''
            for word in words:
                send += word + '\n'

            # Send processed data to server
            requests.post('http://' + '10.10.12.36:5000' + '/getmap', headers={'Content-Type': 'application/json'}, json={'data': send})
        
        # Check if this client is selected as a Reduce Client
        else:
            current_word = None
            current_count = 0
            word = None
            resp = ''
            s = str(data['data'])

            def mysort(line):
                return line.split()[0]

            txt = s.splitlines()
            print("*"*30)
            print(sorted(txt, key=mysort))
            print("*" * 30)

            for line in sorted(txt, key=mysort):
                word, count = line.split('\t',1)

                try:
                    count = int(count)
                except ValueError:                    
                    continue

                if current_word == word:
                    current_count += count
                else:
                    if current_word:
                        resp += current_word + '\t' + str(current_count) + '\n'
                        
                    current_count = count
                    current_word = word

            if current_word == word:                
                resp += current_word + '\t' + str(current_count) + '\n'

            # Send processed data to server
            requests.post('http://' + '10.10.12.36:5000' + '/getreduce', headers={'Content-Type': 'application/json'}, json={'data': resp})
    return 'Client'



if __name__ == '__main__':
    # Run service
    app.run(debug=True, host='10.10.12.42', port=5001)