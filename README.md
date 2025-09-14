# Master's degree project

Repository for the Master's degree project by Mateusz Boboryko

## Useful commands in the project

### Run the web app via a single command

First run <i>npm install</i> in the backend and frontend directories then

<i>./start_webapp.sh</i>


### Python venv - virtual environment

Windows:<br>
open_env\Scripts\activate

deactivate

Linux:<br>
source open_env/bin/activate

pip freeze > requirements.txt

### OpenSearch

To delete existing index on OpenSearch:<br>
curl -k -u 'admin:password' -X DELETE 'https://localhost:9200/university_papers'

OpenSearch version: 2.19.1

To run the script on the VM:<br>
python3.12 script.py

To install requirements on the VM:<br>
pip3.12 install -r requirements.txt

### OpenSearch Dashboard

To enter OpenSearch Dashboard: <br>
(Behind firewall)
VM_IP:5601

### Docker
In the case of stopped docker containers with OpenSearch restart them in a directory with docker compose file with:<br>
docker compose restart 
<br>

check the status with:<br>
docker ps -a

### Runnning front and backend

To get any current git changes:<br>
git fetch<br>
git pull

Backend:<br>
cd MastersProject/WebApp/backend
npm install
node server.js

Frontend:<br>
cd MastersProject/WebApp/frontend
npm install
npm run dev


### Connecting with OpenSearch via Python library OpenSearch-py

https://docs.opensearch.org/docs/latest/clients/python-low-level/
