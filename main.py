from fastapi import FastAPI, Request, Response
import requests

from message_repo import MessageRepo

app = FastAPI()


def get_vm_name():
    response = requests.get('http://169.254.169.254/computeMetadata/v1/instance/id',
                            headers={"Metadata-Flavor": "Google"})
    return response.text


REPLICA_NAME = get_vm_name()
REPLICA_VERSION = '2.0'

MESSAGE_REPO = MessageRepo()


@app.get("/")
async def root(response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = 'true'
    messages = MESSAGE_REPO.get_all()
    return {"replica_version": REPLICA_VERSION,
            "replica_name": REPLICA_NAME,
            "messages": sorted(messages, key=lambda m: m['timestamp'])
            }


@app.post("/")
async def root(request: Request, response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = 'true'
    data = await request.json()
    MESSAGE_REPO.upload(message=data['message'])
    print(data)
    return {"message": data['message'],
            "replica_name": REPLICA_NAME}


@app.get("/ping")
async def ping(request: Request, response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = 'true'
    return "OK"
