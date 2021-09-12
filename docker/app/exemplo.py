from fastapi import FastAPI

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