# Master's degree project

Repository for the Master's degree project by Mateusz Boboryko

## Useful commands in the project

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

## Links used

### Connecting with OpenSearch via Python library OpenSearch-py

https://docs.opensearch.org/docs/latest/clients/python-low-level/
