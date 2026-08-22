
# Master's degree project

Repository for the Master's degree project by me - Mateusz Boboryko.

Full-stack web application serving as a specialized search engine for academic outputs (research papers, authors, and projects) affiliated with the EUNICoast university alliance. The project involved designing and implementing a data pipeline to ingest and process scholarly data from APIs like OpenAlex and OpenAIRE. This data was then indexed into a self-hosted OpenSearch instance, with custom mappings created to optimize search relevance.

The backend was built using Node.js and Express.js, providing a RESTful API to handle complex search queries, filtering, sorting, and aggregations. The frontend was developed with React, featuring a dynamic user interface for keyword searching, faceted navigation (by type, university, etc.), pagination, and customizable result sorting. This project demonstrated proficiency in data ingestion, search engine technology, API development, and modern front-end frameworks, culminating in a functional tool for exploring academic contributions within the EUNICoast alliance.

## Thesis

[Thesis pdf file](https://github.com/user-attachments/files/31338921/PracaMagisterskaMateuszBoboryko_2.pdf)

<b>Abstract of the thesis</b>

This master’s thesis describes the process of designing, implementing, and evaluating a dedicated search engine for the scientific achievements of the 13 universities affiliated with the EUNICoastalliance, as part of the European Universities initiative. The subject of this work was the creation of a system that enables searching for scholarly works, research projects, and author profiles. The project involved an analysis of open data sources, such as OpenAlex and OpenAIRE Graph, and the selection of modern technologies, including the OpenSearch engine, Node.js, and React. A complete search system was implemented, consisting of a data processing pipeline and a web application with a user interface. The result of this work is a functional search engine, the performance of which was verified through a series of tests. These tests confirmed that the system returns relevant results for text-based queries.

keywords: information retrieval, dedicated search engine, web application, open scientific
data, OpenSearch


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
