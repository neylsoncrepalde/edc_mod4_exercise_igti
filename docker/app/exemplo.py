from fastapi import FastAPI
import random

app = FastAPI()

@app.get("/api")
async def root():
    return {"message": "You are doing fine with FastAPI..."}


@app.get("/api/{name}")
async def return_name(name):
    return {
        "name": name,
        "message": f"Hello, {name}!"
    }


@app.get("/piada")
async def return_piada():
    piadas = [
        "O que o cachorro faz quando o dono o chama? Late!",
        "O que cai de pé e corre deitado? Minhoca de pára-quedas!",
        "Em uma corrida de frutas a maçã está ganhando. O que acontece se ela diminuir a velocidade? A uva passa."
    ]
    return {
        "piada": random.choice(piadas)
    }